package test

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	rabbitmq "github.com/rabbitmq/amqp091-go"

	gatewayamqp "github.com/w3nder/whatsmeow-gateway/internal/amqp"
)

type rpcProbe struct {
	ch         *rabbitmq.Channel
	replyQueue string
	replies    <-chan rabbitmq.Delivery
	confirms   chan rabbitmq.Confirmation
}

func newRpcProbe(t *testing.T, conn *rabbitmq.Connection) *rpcProbe {
	t.Helper()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open probe channel: %v", err)
	}
	t.Cleanup(func() { _ = ch.Close() })
	if err := ch.Confirm(false); err != nil {
		t.Fatalf("enable publisher confirms: %v", err)
	}
	confirms := ch.NotifyPublish(make(chan rabbitmq.Confirmation, 1))
	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatalf("declare reply queue: %v", err)
	}
	replies, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume reply queue: %v", err)
	}
	return &rpcProbe{ch: ch, replyQueue: q.Name, replies: replies, confirms: confirms}
}

func (p *rpcProbe) call(t *testing.T, operation, correlationID string, payload string, timeout time.Duration) map[string]any {
	t.Helper()
	if _, err := p.ch.QueueDeclare(gatewayamqp.RpcQueueName(operation), true, false, false, false, rabbitmq.Table{"x-queue-type": "quorum"}); err != nil {
		t.Fatalf("declare rpc queue: %v", err)
	}
	if err := p.ch.PublishWithContext(context.Background(), "", gatewayamqp.RpcQueueName(operation), false, false, rabbitmq.Publishing{
		ContentType:   "application/json",
		ReplyTo:       p.replyQueue,
		CorrelationId: correlationID,
		Expiration:    strconv.FormatInt(timeout.Milliseconds(), 10),
		Body:          []byte(payload),
	}); err != nil {
		t.Fatalf("publish rpc request: %v", err)
	}
	select {
	case confirm := <-p.confirms:
		if !confirm.Ack {
			t.Fatalf("broker nacked publish of %s request (queue not ready?)", operation)
		}
	case <-time.After(timeout + 5*time.Second):
		t.Fatalf("no publish confirm for %s", operation)
	}
	select {
	case d := <-p.replies:
		if d.CorrelationId != correlationID {
			t.Fatalf("reply correlationId %q, want %q", d.CorrelationId, correlationID)
		}
		if d.ContentType != "application/json" || d.DeliveryMode == rabbitmq.Persistent {
			t.Fatalf("reply must be json and transient, got %q / %d", d.ContentType, d.DeliveryMode)
		}
		var reply map[string]any
		if err := json.Unmarshal(d.Body, &reply); err != nil {
			t.Fatalf("reply is not json: %s", d.Body)
		}
		return reply
	case <-time.After(timeout + 5*time.Second):
		t.Fatalf("no reply for %s", operation)
		return nil
	}
}

func TestRpcServerRoundTrip(t *testing.T) {
	conn := startRabbitMQ(t)
	server := gatewayamqp.NewRpcServer(conn, 4)
	t.Cleanup(func() { _ = server.Close() })

	err := server.Handle(context.Background(), "echo.test", func(_ context.Context, payload json.RawMessage) (any, error) {
		var in struct {
			N int `json:"n"`
		}
		if err := json.Unmarshal(payload, &in); err != nil {
			return nil, err
		}
		return map[string]int{"doubled": in.N * 2}, nil
	})
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	probe := newRpcProbe(t, conn)
	reply := probe.call(t, "echo.test", "corr-1", `{"n":21}`, 5*time.Second)
	if reply["ok"] != true || reply["result"].(map[string]any)["doubled"] != float64(42) {
		t.Fatalf("reply %v", reply)
	}
}

func TestRpcServerReportsDomainAndInternalErrors(t *testing.T) {
	conn := startRabbitMQ(t)
	server := gatewayamqp.NewRpcServer(conn, 4)
	t.Cleanup(func() { _ = server.Close() })

	_ = server.Handle(context.Background(), "fail.test", func(_ context.Context, payload json.RawMessage) (any, error) {
		var in struct {
			Kind string `json:"kind"`
		}
		_ = json.Unmarshal(payload, &in)
		if in.Kind == "domain" {
			return nil, gatewayamqp.RpcNotFound("channel is not paired")
		}
		return nil, errBoom
	})

	probe := newRpcProbe(t, conn)
	domain := probe.call(t, "fail.test", "corr-d", `{"kind":"domain"}`, 5*time.Second)
	if domain["ok"] != false || domain["error"].(map[string]any)["code"] != "not_found" {
		t.Fatalf("domain reply %v", domain)
	}
	internal := probe.call(t, "fail.test", "corr-i", `{"kind":"other"}`, 5*time.Second)
	if internal["ok"] != false || internal["error"].(map[string]any)["code"] != "internal" {
		t.Fatalf("internal reply %v", internal)
	}
	invalid := probe.call(t, "fail.test", "corr-j", `{not json`, 5*time.Second)
	if invalid["ok"] != false || invalid["error"].(map[string]any)["code"] != "invalid_request" {
		t.Fatalf("invalid reply %v", invalid)
	}
}

func TestRpcServerSkipsRequestsThatAlreadyExpired(t *testing.T) {
	conn := startRabbitMQ(t)
	probe := newRpcProbe(t, conn)

	if _, err := probe.ch.QueueDeclare(gatewayamqp.RpcQueueName("late.test"), true, false, false, false, rabbitmq.Table{"x-queue-type": "quorum"}); err != nil {
		t.Fatalf("declare: %v", err)
	}
	if err := probe.ch.PublishWithContext(context.Background(), "", gatewayamqp.RpcQueueName("late.test"), false, false, rabbitmq.Publishing{
		ContentType: "application/json", ReplyTo: probe.replyQueue, CorrelationId: "corr-late", Expiration: "60000", Body: []byte(`{}`),
		Timestamp: time.Now().Add(-2 * time.Minute),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	handled := make(chan struct{}, 1)
	server := gatewayamqp.NewRpcServer(conn, 4)
	t.Cleanup(func() { _ = server.Close() })
	_ = server.Handle(context.Background(), "late.test", func(context.Context, json.RawMessage) (any, error) {
		handled <- struct{}{}
		return map[string]bool{"ran": true}, nil
	})

	select {
	case <-handled:
		t.Fatal("an expired request must not reach the handler")
	case d := <-probe.replies:
		t.Fatalf("an expired request must not be answered, got %s", d.Body)
	case <-time.After(3 * time.Second):
	}
}

func TestRpcServerRecoversFromHandlerPanic(t *testing.T) {
	conn := startRabbitMQ(t)
	server := gatewayamqp.NewRpcServer(conn, 4)
	t.Cleanup(func() { _ = server.Close() })

	err := server.Handle(context.Background(), "panic.test", func(context.Context, json.RawMessage) (any, error) {
		panic("boom")
	})
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	probe := newRpcProbe(t, conn)
	first := probe.call(t, "panic.test", "corr-panic-1", `{}`, 5*time.Second)
	if first["ok"] != false || first["error"].(map[string]any)["code"] != "internal" {
		t.Fatalf("panic reply %v", first)
	}

	second := probe.call(t, "panic.test", "corr-panic-2", `{}`, 5*time.Second)
	if second["ok"] != false || second["error"].(map[string]any)["code"] != "internal" {
		t.Fatalf("second panic reply %v", second)
	}
}

func TestRpcServerRepliesInternalWhenResultIsNotSerializable(t *testing.T) {
	conn := startRabbitMQ(t)
	server := gatewayamqp.NewRpcServer(conn, 4)
	t.Cleanup(func() { _ = server.Close() })

	err := server.Handle(context.Background(), "unserializable.test", func(context.Context, json.RawMessage) (any, error) {
		return map[string]any{"ch": make(chan int)}, nil
	})
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	probe := newRpcProbe(t, conn)
	reply := probe.call(t, "unserializable.test", "corr-unserializable", `{}`, 5*time.Second)
	if reply["ok"] != false || reply["error"].(map[string]any)["code"] != "internal" {
		t.Fatalf("unserializable reply %v", reply)
	}
}

var errBoom = errors.New("boom")
