package amqp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	rabbitmq "github.com/rabbitmq/amqp091-go"
)

type SendHandler func(ctx context.Context, cmd GatewaySendCommand) error

// PairHandler follows one pairing from the command that asks for it to the end of the
// session it starts, which is the operator's twenty to sixty seconds with a QR on
// screen -- far longer than a command may hold the queue.
//
// So the handler settles its own delivery. It calls accept as soon as the pairing is
// under way, and from then on the session reports itself as channel events; the error
// it returns afterwards is the handler's own to log, since the command is long
// answered. Returning before accept is the one remaining way to say the pairing could
// not be started at all, and that command is dead-lettered as every failed command was.
type PairHandler func(ctx context.Context, cmd PairCommand, accept func()) error

type CallHandler func(ctx context.Context, cmd GatewayCallCommand) error

type ConsumerConfig struct {
	Prefetch int
}

type Consumer struct {
	sendCh *rabbitmq.Channel
	pairCh *rabbitmq.Channel
	callCh *rabbitmq.Channel

	sendStarted bool
	pairStarted bool
	callStarted bool

	closing atomic.Bool
	failed  chan error

	wg sync.WaitGroup
}

// Failed reports a consumer that stopped on its own. A broker restart or a channel-level
// error closes the delivery channel, which ends the range loop below; without this the
// gateway would keep running while consuming nothing, acking nothing, and logging
// nothing. Buffered so the reporting goroutine never blocks, and only the first failure
// is kept -- the connection dying takes both consumers down at once.
func (c *Consumer) Failed() <-chan error {
	return c.failed
}

func (c *Consumer) reportFailure(err error) {
	if c.closing.Load() {
		return
	}
	select {
	case c.failed <- err:
	default:
	}
}

func NewConsumer(conn *rabbitmq.Connection, cfg ConsumerConfig) (*Consumer, error) {
	sendCh, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("amqp: open gateway.send channel: %w", err)
	}
	if err := declareCommandTopology(sendCh, GatewaySendExchange, GatewaySendQueue, GatewaySendDLX, GatewaySendDLQ); err != nil {
		_ = sendCh.Close()
		return nil, err
	}
	if err := sendCh.Qos(cfg.Prefetch, 0, false); err != nil {
		_ = sendCh.Close()
		return nil, fmt.Errorf("amqp: set qos on gateway.send channel: %w", err)
	}

	pairCh, err := conn.Channel()
	if err != nil {
		_ = sendCh.Close()
		return nil, fmt.Errorf("amqp: open gateway.pair channel: %w", err)
	}
	if err := declareCommandTopology(pairCh, GatewayPairExchange, GatewayPairQueue, GatewayPairDLX, GatewayPairDLQ); err != nil {
		_ = sendCh.Close()
		_ = pairCh.Close()
		return nil, err
	}
	if err := pairCh.Qos(cfg.Prefetch, 0, false); err != nil {
		_ = sendCh.Close()
		_ = pairCh.Close()
		return nil, fmt.Errorf("amqp: set qos on gateway.pair channel: %w", err)
	}

	callCh, err := conn.Channel()
	if err != nil {
		_ = sendCh.Close()
		_ = pairCh.Close()
		return nil, fmt.Errorf("amqp: open gateway.call channel: %w", err)
	}
	if err := declareCommandTopology(callCh, GatewayCallExchange, GatewayCallQueue, GatewayCallDLX, GatewayCallDLQ); err != nil {
		_ = sendCh.Close()
		_ = pairCh.Close()
		_ = callCh.Close()
		return nil, err
	}
	if err := callCh.Qos(cfg.Prefetch, 0, false); err != nil {
		_ = sendCh.Close()
		_ = pairCh.Close()
		_ = callCh.Close()
		return nil, fmt.Errorf("amqp: set qos on gateway.call channel: %w", err)
	}

	return &Consumer{
		sendCh: sendCh,
		pairCh: pairCh,
		callCh: callCh,
		failed: make(chan error, 1),
	}, nil
}

func (c *Consumer) StartCall(ctx context.Context, handler CallHandler) error {
	deliveries, err := c.callCh.Consume(GatewayCallQueue, GatewayCallConsumer, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("amqp: consume %s: %w", GatewayCallQueue, err)
	}
	c.callStarted = true
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for d := range deliveries {
			var cmd GatewayCallCommand
			handlerErr := json.Unmarshal(d.Body, &cmd)
			if handlerErr == nil {
				handlerErr = handler(ctx, cmd)
			}
			settle(d, handlerErr)
		}
		c.reportFailure(fmt.Errorf("amqp: %s consumer stopped: broker closed the delivery channel", GatewayCallQueue))
	}()
	return nil
}

func (c *Consumer) StartSend(ctx context.Context, handler SendHandler) error {
	deliveries, err := c.sendCh.Consume(GatewaySendQueue, GatewaySendConsumer, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("amqp: consume %s: %w", GatewaySendQueue, err)
	}
	c.sendStarted = true
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for d := range deliveries {
			var cmd GatewaySendCommand
			handlerErr := json.Unmarshal(d.Body, &cmd)
			if handlerErr == nil {
				handlerErr = handler(ctx, cmd)
			}
			settle(d, handlerErr)
		}
		c.reportFailure(fmt.Errorf("amqp: %s consumer stopped: broker closed the delivery channel", GatewaySendQueue))
	}()
	return nil
}

// StartPair consumes pair commands, following each pairing on a worker of its own.
//
// Unlike a send or a call command, a pairing waits on a person: the session runs until
// the operator scans the code or it times out. Running it inline held this consumer for
// that whole minute, so the same operator reopening the pairing dialog -- and every
// other channel's pairing on this instance -- sat unread on the queue behind it. Handing
// the session to a worker frees the loop to admit the next command at once.
//
// Nothing here caps how many sessions run at once, and nothing here may: the channels
// on this queue belong to independent tenants, and a cap makes the operator past it wait
// out somebody else's pairing -- the very stall this worker exists to remove.
//
// What bounds the work is the session manager: a channel can only have one pairing
// session, so a second pair command for a channel joins the live one instead of opening
// another, and duplicates and stale commands collapse rather than multiply. Since each
// live session holds one WhatsApp websocket, that collapse is what keeps sockets
// proportional to the operators actually pairing rather than to the commands sent.
func (c *Consumer) StartPair(ctx context.Context, handler PairHandler) error {
	deliveries, err := c.pairCh.Consume(GatewayPairQueue, GatewayPairConsumer, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("amqp: consume %s: %w", GatewayPairQueue, err)
	}
	c.pairStarted = true
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		// The loop goroutine outlives the deliveries by however long the last sessions
		// take, so that Close -- which waits on c.wg -- never reports a drained
		// consumer while a pairing is still running.
		var sessions sync.WaitGroup
		defer sessions.Wait()

		for d := range deliveries {
			var cmd PairCommand
			if err := json.Unmarshal(d.Body, &cmd); err != nil {
				settle(d, err)
				continue
			}

			sessions.Add(1)
			go func() {
				defer sessions.Done()
				runPairSession(ctx, handler, cmd, d)
			}()
		}

		// Reported before the running sessions are waited on: the broker is gone, and
		// the gateway has to hear that now rather than after the last operator's minute
		// with a QR runs out.
		c.reportFailure(fmt.Errorf("amqp: %s consumer stopped: broker closed the delivery channel", GatewayPairQueue))
	}()
	return nil
}

// runPairSession settles a pair delivery once, at whichever comes first: the handler
// accepting the pairing, or the handler giving up before it started one.
//
// That split is deliberate. A pairing that never started is an instance-level failure --
// no device, no QR channel, no socket -- and dead-lettering it keeps that visible. A
// pairing that started and then failed is the operator not scanning in time, which is
// already on their screen as a channel event; nacking it too would fill the DLQ with
// commands no one can act on and hide the failures that do need acting on.
func runPairSession(ctx context.Context, handler PairHandler, cmd PairCommand, d rabbitmq.Delivery) {
	var settled sync.Once
	accept := func() {
		settled.Do(func() { _ = d.Ack(false) })
	}

	err := handler(ctx, cmd, accept)
	settled.Do(func() { settle(d, err) })
}

func settle(d rabbitmq.Delivery, err error) {
	if err != nil {
		_ = d.Nack(false, false)
		return
	}
	_ = d.Ack(false)
}

func (c *Consumer) Close() error {
	// Set before cancelling: cancelling closes the delivery channels too, and that must
	// not be mistaken for the broker dying.
	c.closing.Store(true)

	var errs []error
	if c.sendStarted {
		if err := c.sendCh.Cancel(GatewaySendConsumer, false); err != nil {
			errs = append(errs, fmt.Errorf("amqp: cancel %s consumer: %w", GatewaySendQueue, err))
		}
	}
	if c.pairStarted {
		if err := c.pairCh.Cancel(GatewayPairConsumer, false); err != nil {
			errs = append(errs, fmt.Errorf("amqp: cancel %s consumer: %w", GatewayPairQueue, err))
		}
	}
	if c.callStarted {
		if err := c.callCh.Cancel(GatewayCallConsumer, false); err != nil {
			errs = append(errs, fmt.Errorf("amqp: cancel %s consumer: %w", GatewayCallQueue, err))
		}
	}
	c.wg.Wait()
	if err := c.sendCh.Close(); err != nil {
		errs = append(errs, fmt.Errorf("amqp: close gateway.send channel: %w", err))
	}
	if err := c.pairCh.Close(); err != nil {
		errs = append(errs, fmt.Errorf("amqp: close gateway.pair channel: %w", err))
	}
	if err := c.callCh.Close(); err != nil {
		errs = append(errs, fmt.Errorf("amqp: close gateway.call channel: %w", err))
	}
	return errors.Join(errs...)
}
