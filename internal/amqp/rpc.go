package amqp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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
	RpcCodeLocked         = "locked"
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
func RpcLocked(message string) error     { return &RpcError{Code: RpcCodeLocked, Message: message} }
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
	logger   *slog.Logger

	mu       sync.Mutex
	channels map[string]*rabbitmq.Channel

	closing    atomic.Bool
	failed     chan error
	failedOnce sync.Once
	wg         sync.WaitGroup
}

func NewRpcServer(conn *rabbitmq.Connection, prefetch int, logger *slog.Logger) *RpcServer {
	return &RpcServer{conn: conn, prefetch: prefetch, logger: logger, channels: make(map[string]*rabbitmq.Channel), failed: make(chan error, 1)}
}

func (s *RpcServer) Failed() <-chan error {
	return s.failed
}

func (s *RpcServer) Handle(ctx context.Context, operation string, handler RpcHandler) error {
	if !s.reserve(operation) {
		return fmt.Errorf("amqp: rpc %s already has a handler", operation)
	}
	ch, deliveries, err := s.open(operation)
	if err != nil {
		s.release(operation)
		return err
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
				s.serve(ctx, operation, ch, d, handler)
			}(d)
		}
		s.reportFailure(fmt.Errorf("amqp: %s consumer stopped: broker closed the delivery channel", RpcQueueName(operation)))
	}()
	return nil
}

func (s *RpcServer) reserve(operation string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, taken := s.channels[operation]; taken {
		return false
	}
	s.channels[operation] = nil
	return true
}

func (s *RpcServer) release(operation string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.channels, operation)
}

func (s *RpcServer) open(operation string) (*rabbitmq.Channel, <-chan rabbitmq.Delivery, error) {
	ch, err := s.conn.Channel()
	if err != nil {
		return nil, nil, fmt.Errorf("amqp: open rpc channel for %s: %w", operation, err)
	}
	queue := RpcQueueName(operation)
	if _, err := ch.QueueDeclare(queue, true, false, false, false, rabbitmq.Table{"x-queue-type": "quorum"}); err != nil {
		_ = ch.Close()
		return nil, nil, fmt.Errorf("amqp: declare %s: %w", queue, err)
	}
	if err := ch.Qos(s.prefetch, 0, false); err != nil {
		_ = ch.Close()
		return nil, nil, fmt.Errorf("amqp: set qos on %s: %w", queue, err)
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, nil, fmt.Errorf("amqp: enable reply confirms on %s: %w", queue, err)
	}
	deliveries, err := ch.Consume(queue, "whatsmeow-gateway.rpc."+operation, false, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		return nil, nil, fmt.Errorf("amqp: consume %s: %w", queue, err)
	}
	return ch, deliveries, nil
}

const replyPublishTimeout = 5 * time.Second

func (s *RpcServer) serve(ctx context.Context, operation string, ch *rabbitmq.Channel, d rabbitmq.Delivery, handler RpcHandler) {
	now := time.Now()
	if expired(d, now) {
		s.ack(operation, d)
		return
	}
	callCtx, cancel := context.WithDeadline(ctx, requestDeadline(d, now))
	defer cancel()

	reply := answer(callCtx, d.Body, handler)
	if d.ReplyTo != "" {
		if err := s.publishReply(ctx, ch, d, reply); err != nil {
			s.logger.Error("amqp: rpc reply not delivered, the caller will time out", "operation", operation, "correlation_id", d.CorrelationId, "error", err)
		}
	}
	s.ack(operation, d)
}

func (s *RpcServer) publishReply(ctx context.Context, ch *rabbitmq.Channel, d rabbitmq.Delivery, reply rpcReply) error {
	body, err := json.Marshal(reply)
	if err != nil {
		body, _ = json.Marshal(rpcReply{OK: false, Error: &rpcErrorBody{Code: RpcCodeInternal, Message: "reply is not serializable: " + err.Error()}})
	}
	publishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), replyPublishTimeout)
	defer cancel()
	confirm, err := ch.PublishWithDeferredConfirmWithContext(publishCtx, "", d.ReplyTo, false, false, rabbitmq.Publishing{
		ContentType:   "application/json",
		CorrelationId: d.CorrelationId,
		DeliveryMode:  rabbitmq.Transient,
		Body:          body,
	})
	if err != nil {
		return fmt.Errorf("publish reply: %w", err)
	}
	acked, err := confirm.WaitContext(publishCtx)
	if err != nil {
		return fmt.Errorf("wait reply confirm: %w", err)
	}
	if !acked {
		return errors.New("broker refused the reply")
	}
	return nil
}

func (s *RpcServer) ack(operation string, d rabbitmq.Delivery) {
	if err := d.Ack(false); err != nil {
		s.logger.Error("amqp: ack rpc request", "operation", operation, "correlation_id", d.CorrelationId, "error", err)
	}
}

func answer(ctx context.Context, body []byte, handler RpcHandler) (reply rpcReply) {
	if !json.Valid(body) {
		return rpcReply{OK: false, Error: &rpcErrorBody{Code: RpcCodeInvalidRequest, Message: "request is not valid JSON"}}
	}
	defer func() {
		if r := recover(); r != nil {
			reply = rpcReply{OK: false, Error: &rpcErrorBody{Code: RpcCodeInternal, Message: fmt.Sprintf("handler panicked: %v", r)}}
		}
	}()
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

const (
	defaultRpcTimeout   = 30 * time.Second
	timestampResolution = time.Second
)

func requestTimeout(d rabbitmq.Delivery) time.Duration {
	ms, err := strconv.ParseInt(d.Expiration, 10, 64)
	if err != nil || ms <= 0 {
		return defaultRpcTimeout
	}
	return time.Duration(ms) * time.Millisecond
}

func requestDeadline(d rabbitmq.Delivery, now time.Time) time.Time {
	if d.Timestamp.IsZero() || d.Expiration == "" {
		return now.Add(requestTimeout(d))
	}
	return d.Timestamp.Add(timestampResolution + requestTimeout(d))
}

func expired(d rabbitmq.Delivery, now time.Time) bool {
	return !now.Before(requestDeadline(d, now))
}

func (s *RpcServer) reportFailure(err error) {
	if s.closing.Load() {
		return
	}
	s.logger.Error("amqp: rpc consumer failed", "error", err)
	s.failedOnce.Do(func() { s.failed <- err })
}

func (s *RpcServer) Close() error {
	s.closing.Store(true)
	s.mu.Lock()
	channels := s.channels
	s.channels = make(map[string]*rabbitmq.Channel)
	s.mu.Unlock()

	var errs []error
	for operation, ch := range channels {
		if ch == nil {
			continue
		}
		if err := ch.Cancel("whatsmeow-gateway.rpc."+operation, false); err != nil {
			errs = append(errs, fmt.Errorf("amqp: cancel rpc %s: %w", operation, err))
		}
	}
	s.wg.Wait()
	for operation, ch := range channels {
		if ch == nil {
			continue
		}
		if err := ch.Close(); err != nil {
			errs = append(errs, fmt.Errorf("amqp: close rpc channel %s: %w", operation, err))
		}
	}
	return errors.Join(errs...)
}
