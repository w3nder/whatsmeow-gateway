package test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	rabbitmq "github.com/rabbitmq/amqp091-go"
	tcrabbitmq "github.com/testcontainers/testcontainers-go/modules/rabbitmq"

	gatewayamqp "github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/mapper"
)

const rabbitMQImage = "rabbitmq:3.12.11-management-alpine"

func startRabbitMQ(t *testing.T) *rabbitmq.Connection {
	t.Helper()
	ctx := context.Background()

	container, err := tcrabbitmq.Run(ctx, rabbitMQImage)
	if err != nil {
		t.Fatalf("failed to start rabbitmq container: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Errorf("failed to terminate rabbitmq container: %v", err)
		}
	})

	url, err := container.AmqpURL(ctx)
	if err != nil {
		t.Fatalf("failed to get amqp url: %v", err)
	}

	conn, err := rabbitmq.Dial(url)
	if err != nil {
		t.Fatalf("failed to dial rabbitmq: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil && !errors.Is(err, rabbitmq.ErrClosed) {
			t.Errorf("failed to close rabbitmq connection: %v", err)
		}
	})

	return conn
}

func TestPublisherPublishesInboundEventExactShape(t *testing.T) {
	conn := startRabbitMQ(t)

	publisher, err := gatewayamqp.NewPublisher(conn)
	if err != nil {
		t.Fatalf("NewPublisher failed: %v", err)
	}
	t.Cleanup(func() {
		if err := publisher.Close(); err != nil {
			t.Errorf("publisher.Close failed: %v", err)
		}
	})

	probeCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open probe channel: %v", err)
	}
	t.Cleanup(func() {
		if err := probeCh.Close(); err != nil {
			t.Errorf("failed to close probe channel: %v", err)
		}
	})

	q, err := probeCh.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatalf("failed to declare probe queue: %v", err)
	}
	if err := probeCh.QueueBind(q.Name, gatewayamqp.InboundRoutingKey, gatewayamqp.EventsExchange, false, nil); err != nil {
		t.Fatalf("failed to bind probe queue: %v", err)
	}

	deliveries, err := probeCh.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to consume probe queue: %v", err)
	}

	evt := mapper.InboundEvent{
		PhoneNumberID:     "channel-1",
		From:              "5511999999999",
		ProfileName:       "Jane",
		ProviderMessageID: "wamid.abc",
		Timestamp:         "1700000000",
		Type:              "text",
		Text:              &mapper.InboundText{Body: "hello"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := publisher.PublishInbound(ctx, evt); err != nil {
		t.Fatalf("PublishInbound failed: %v", err)
	}

	select {
	case d := <-deliveries:
		if d.ContentType != "application/json" {
			t.Fatalf("expected ContentType application/json, got %q", d.ContentType)
		}
		if d.DeliveryMode != rabbitmq.Persistent {
			t.Fatalf("expected DeliveryMode Persistent, got %d", d.DeliveryMode)
		}

		var raw map[string]any
		if err := json.Unmarshal(d.Body, &raw); err != nil {
			t.Fatalf("failed to unmarshal raw body: %v", err)
		}
		if raw["phoneNumberId"] != "channel-1" {
			t.Fatalf("expected phoneNumberId=channel-1 on wire, got %v", raw["phoneNumberId"])
		}
		if raw["providerMessageId"] != "wamid.abc" {
			t.Fatalf("expected providerMessageId=wamid.abc on wire, got %v", raw["providerMessageId"])
		}

		var got mapper.InboundEvent
		if err := json.Unmarshal(d.Body, &got); err != nil {
			t.Fatalf("failed to unmarshal InboundEvent: %v", err)
		}
		if !reflect.DeepEqual(got, evt) {
			t.Fatalf("expected round-tripped event %+v, got %+v", evt, got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for inbound event on probe queue")
	}
}

func TestConsumerHandlesGatewaySendCommand(t *testing.T) {
	conn := startRabbitMQ(t)

	consumer, err := gatewayamqp.NewConsumer(conn, gatewayamqp.ConsumerConfig{Prefetch: 10})
	if err != nil {
		t.Fatalf("NewConsumer failed: %v", err)
	}
	t.Cleanup(func() {
		if err := consumer.Close(); err != nil {
			t.Errorf("consumer.Close failed: %v", err)
		}
	})

	received := make(chan gatewayamqp.GatewaySendCommand, 1)
	err = consumer.StartSend(context.Background(), func(_ context.Context, cmd gatewayamqp.GatewaySendCommand) error {
		received <- cmd
		return nil
	})
	if err != nil {
		t.Fatalf("StartSend failed: %v", err)
	}

	publishCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open publish channel: %v", err)
	}
	t.Cleanup(func() {
		if err := publishCh.Close(); err != nil {
			t.Errorf("failed to close publish channel: %v", err)
		}
	})

	cmd := gatewayamqp.GatewaySendCommand{
		TenantID:  "tenant-1",
		ChannelID: "channel-1",
		MessageID: "msg-1",
		To:        "+5511999999999",
		Type:      "text",
		Text:      "hi",
	}
	body, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("failed to marshal command: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = publishCh.PublishWithContext(ctx, gatewayamqp.GatewaySendExchange, "42", false, false, rabbitmq.Publishing{
		ContentType:  "application/json",
		DeliveryMode: rabbitmq.Persistent,
		Body:         body,
	})
	if err != nil {
		t.Fatalf("failed to publish gateway send command: %v", err)
	}

	select {
	case got := <-received:
		if !reflect.DeepEqual(got, cmd) {
			t.Fatalf("expected handler to receive %+v, got %+v", cmd, got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for consumer to hand off the gateway send command")
	}
}

func TestConsumerHandlerErrorRoutesToDLQAndDoesNotRetryLocally(t *testing.T) {
	conn := startRabbitMQ(t)

	consumer, err := gatewayamqp.NewConsumer(conn, gatewayamqp.ConsumerConfig{Prefetch: 10})
	if err != nil {
		t.Fatalf("NewConsumer failed: %v", err)
	}
	t.Cleanup(func() {
		if err := consumer.Close(); err != nil {
			t.Errorf("consumer.Close failed: %v", err)
		}
	})

	var callCount int32
	err = consumer.StartSend(context.Background(), func(_ context.Context, _ gatewayamqp.GatewaySendCommand) error {
		atomic.AddInt32(&callCount, 1)
		return errors.New("boom")
	})
	if err != nil {
		t.Fatalf("StartSend failed: %v", err)
	}

	dlqCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open dlq channel: %v", err)
	}
	t.Cleanup(func() {
		if err := dlqCh.Close(); err != nil {
			t.Errorf("failed to close dlq channel: %v", err)
		}
	})

	dlqDeliveries, err := dlqCh.Consume(gatewayamqp.GatewaySendDLQ, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to consume dlq: %v", err)
	}

	publishCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open publish channel: %v", err)
	}
	t.Cleanup(func() {
		if err := publishCh.Close(); err != nil {
			t.Errorf("failed to close publish channel: %v", err)
		}
	})

	cmd := gatewayamqp.GatewaySendCommand{
		TenantID:  "tenant-1",
		ChannelID: "channel-1",
		MessageID: "msg-err",
		To:        "+5511999999999",
		Type:      "text",
		Text:      "boom",
	}
	body, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("failed to marshal command: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = publishCh.PublishWithContext(ctx, gatewayamqp.GatewaySendExchange, "7", false, false, rabbitmq.Publishing{
		ContentType:  "application/json",
		DeliveryMode: rabbitmq.Persistent,
		Body:         body,
	})
	if err != nil {
		t.Fatalf("failed to publish gateway send command: %v", err)
	}

	select {
	case d := <-dlqDeliveries:
		var got gatewayamqp.GatewaySendCommand
		if err := json.Unmarshal(d.Body, &got); err != nil {
			t.Fatalf("failed to unmarshal dead-lettered command: %v", err)
		}
		if !reflect.DeepEqual(got, cmd) {
			t.Fatalf("expected dead-lettered command %+v, got %+v", cmd, got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for message on gateway.send.dlq")
	}

	time.Sleep(200 * time.Millisecond)
	if got := atomic.LoadInt32(&callCount); got != 1 {
		t.Fatalf("expected the handler to run exactly once (no local retry loop before dead-lettering), got %d calls", got)
	}
}

func TestConsumerCloseWaitsForInFlightHandler(t *testing.T) {
	conn := startRabbitMQ(t)

	consumer, err := gatewayamqp.NewConsumer(conn, gatewayamqp.ConsumerConfig{Prefetch: 10})
	if err != nil {
		t.Fatalf("NewConsumer failed: %v", err)
	}

	started := make(chan struct{})
	var finished int32
	err = consumer.StartSend(context.Background(), func(_ context.Context, _ gatewayamqp.GatewaySendCommand) error {
		close(started)
		time.Sleep(500 * time.Millisecond)
		atomic.StoreInt32(&finished, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("StartSend failed: %v", err)
	}

	publishCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open publish channel: %v", err)
	}
	t.Cleanup(func() {
		if err := publishCh.Close(); err != nil {
			t.Errorf("failed to close publish channel: %v", err)
		}
	})

	cmd := gatewayamqp.GatewaySendCommand{TenantID: "t", ChannelID: "c", MessageID: "m", To: "+1", Type: "text", Text: "x"}
	body, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("failed to marshal command: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = publishCh.PublishWithContext(ctx, gatewayamqp.GatewaySendExchange, "1", false, false, rabbitmq.Publishing{
		ContentType:  "application/json",
		DeliveryMode: rabbitmq.Persistent,
		Body:         body,
	})
	if err != nil {
		t.Fatalf("failed to publish gateway send command: %v", err)
	}

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never started")
	}

	if err := consumer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if atomic.LoadInt32(&finished) != 1 {
		t.Fatal("Close returned before the in-flight handler finished")
	}
}
