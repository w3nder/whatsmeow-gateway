package amqp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	rabbitmq "github.com/rabbitmq/amqp091-go"
)

const (
	RpcCodeInvalidRequest = "invalid_request"
	RpcCodeNotFound       = "not_found"
	RpcCodeUnavailable    = "unavailable"
	RpcCodeBadGateway     = "bad_gateway"
	RpcCodeInternal       = "internal"
)

type RpcError struct {
	Code    string
	Message string
}

func (e *RpcError) Error() string {
	return e.Code + ": " + e.Message
}

func RpcNotFound(message string) error { return &RpcError{Code: RpcCodeNotFound, Message: message} }
func RpcUnavailable(message string) error {
	return &RpcError{Code: RpcCodeUnavailable, Message: message}
}
func RpcBadGateway(message string) error { return &RpcError{Code: RpcCodeBadGateway, Message: message} }
func RpcInvalidRequest(message string) error {
	return &RpcError{Code: RpcCodeInvalidRequest, Message: message}
}

type RpcHandler func(ctx context.Context, payload json.RawMessage) (any, error)

type rpcReply struct {
	OK     bool          `json:"ok"`
	Result any           `json:"result,omitempty"`
	Error  *rpcErrorBody `json:"error,omitempty"`
}

type rpcErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type RpcServer struct {
	conn     *rabbitmq.Connection
	prefetch int

	mu       sync.Mutex
	channels map[string]*rabbitmq.Channel

	closing atomic.Bool
	failed  chan error
	wg      sync.WaitGroup
}

func NewRpcServer(conn *rabbitmq.Connection, prefetch int) *RpcServer {
	return &RpcServer{conn: conn, prefetch: prefetch, channels: make(map[string]*rabbitmq.Channel), failed: make(chan error, 1)}
}

func (s *RpcServer) Failed() <-chan error {
	return s.failed
}

func (s *RpcServer) Handle(ctx context.Context, operation string, handler RpcHandler) error {
	ch, err := s.conn.Channel()
	if err != nil {
		return fmt.Errorf("amqp: open rpc channel for %s: %w", operation, err)
	}
	queue := RpcQueueName(operation)
	if _, err := ch.QueueDeclare(queue, true, false, false, false, rabbitmq.Table{"x-queue-type": "quorum"}); err != nil {
		_ = ch.Close()
		return fmt.Errorf("amqp: declare %s: %w", queue, err)
	}
	if err := ch.Qos(s.prefetch, 0, false); err != nil {
		_ = ch.Close()
		return fmt.Errorf("amqp: set qos on %s: %w", queue, err)
	}
	deliveries, err := ch.Consume(queue, "whatsmeow-gateway.rpc."+operation, false, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		return fmt.Errorf("amqp: consume %s: %w", queue, err)
	}

	s.mu.Lock()
	s.channels[operation] = ch
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		var inflight sync.WaitGroup
		defer inflight.Wait()
		for d := range deliveries {
			inflight.Add(1)
			go func(d rabbitmq.Delivery) {
				defer inflight.Done()
				s.serve(ctx, ch, d, handler)
			}(d)
		}
		s.reportFailure(fmt.Errorf("amqp: %s consumer stopped: broker closed the delivery channel", queue))
	}()
	return nil
}

func (s *RpcServer) serve(ctx context.Context, ch *rabbitmq.Channel, d rabbitmq.Delivery, handler RpcHandler) {
	if expired(d, time.Now()) {
		_ = d.Ack(false)
		return
	}
	callCtx, cancel := context.WithTimeout(ctx, requestTimeout(d))
	defer cancel()

	reply := answer(callCtx, d.Body, handler)
	if d.ReplyTo != "" {
		body, _ := json.Marshal(reply)
		if err := ch.PublishWithContext(ctx, "", d.ReplyTo, false, false, rabbitmq.Publishing{
			ContentType:   "application/json",
			CorrelationId: d.CorrelationId,
			DeliveryMode:  rabbitmq.Transient,
			Body:          body,
		}); err != nil {
			_ = d.Nack(false, false)
			return
		}
	}
	_ = d.Ack(false)
}

func answer(ctx context.Context, body []byte, handler RpcHandler) rpcReply {
	if !json.Valid(body) {
		return rpcReply{OK: false, Error: &rpcErrorBody{Code: RpcCodeInvalidRequest, Message: "request is not valid JSON"}}
	}
	result, err := handler(ctx, body)
	if err == nil {
		return rpcReply{OK: true, Result: result}
	}
	var rpcErr *RpcError
	if errors.As(err, &rpcErr) {
		return rpcReply{OK: false, Error: &rpcErrorBody{Code: rpcErr.Code, Message: rpcErr.Message}}
	}
	return rpcReply{OK: false, Error: &rpcErrorBody{Code: RpcCodeInternal, Message: err.Error()}}
}

const defaultRpcTimeout = 30 * time.Second

func requestTimeout(d rabbitmq.Delivery) time.Duration {
	ms, err := strconv.ParseInt(d.Expiration, 10, 64)
	if err != nil || ms <= 0 {
		return defaultRpcTimeout
	}
	return time.Duration(ms) * time.Millisecond
}

func expired(d rabbitmq.Delivery, now time.Time) bool {
	if d.Timestamp.IsZero() || d.Expiration == "" {
		return false
	}
	return now.After(d.Timestamp.Add(requestTimeout(d)))
}

func (s *RpcServer) reportFailure(err error) {
	if s.closing.Load() {
		return
	}
	select {
	case s.failed <- err:
	default:
	}
}

func (s *RpcServer) Close() error {
	s.closing.Store(true)
	s.mu.Lock()
	channels := s.channels
	s.channels = make(map[string]*rabbitmq.Channel)
	s.mu.Unlock()

	var errs []error
	for operation, ch := range channels {
		if err := ch.Cancel("whatsmeow-gateway.rpc."+operation, false); err != nil {
			errs = append(errs, fmt.Errorf("amqp: cancel rpc %s: %w", operation, err))
		}
	}
	s.wg.Wait()
	for operation, ch := range channels {
		if err := ch.Close(); err != nil {
			errs = append(errs, fmt.Errorf("amqp: close rpc channel %s: %w", operation, err))
		}
	}
	return errors.Join(errs...)
}
