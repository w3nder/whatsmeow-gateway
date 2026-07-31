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

type PairHandler func(ctx context.Context, cmd PairCommand) error

type ConsumerConfig struct {
	Prefetch int
}

type Consumer struct {
	sendCh *rabbitmq.Channel
	pairCh *rabbitmq.Channel

	sendStarted bool
	pairStarted bool

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

	return &Consumer{sendCh: sendCh, pairCh: pairCh, failed: make(chan error, 1)}, nil
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

func (c *Consumer) StartPair(ctx context.Context, handler PairHandler) error {
	deliveries, err := c.pairCh.Consume(GatewayPairQueue, GatewayPairConsumer, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("amqp: consume %s: %w", GatewayPairQueue, err)
	}
	c.pairStarted = true
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for d := range deliveries {
			var cmd PairCommand
			handlerErr := json.Unmarshal(d.Body, &cmd)
			if handlerErr == nil {
				handlerErr = handler(ctx, cmd)
			}
			settle(d, handlerErr)
		}
		c.reportFailure(fmt.Errorf("amqp: %s consumer stopped: broker closed the delivery channel", GatewayPairQueue))
	}()
	return nil
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
	c.wg.Wait()
	if err := c.sendCh.Close(); err != nil {
		errs = append(errs, fmt.Errorf("amqp: close gateway.send channel: %w", err))
	}
	if err := c.pairCh.Close(); err != nil {
		errs = append(errs, fmt.Errorf("amqp: close gateway.pair channel: %w", err))
	}
	return errors.Join(errs...)
}
