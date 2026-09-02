package test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	rabbitmq "github.com/rabbitmq/amqp091-go"

	gatewayamqp "github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/call"
)

func TestConsumerDeliversCallCommand(t *testing.T) {
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

	received := make(chan gatewayamqp.GatewayCallCommand, 1)
	if err := consumer.StartCall(context.Background(), func(_ context.Context, cmd gatewayamqp.GatewayCallCommand) error {
		received <- cmd
		return nil
	}); err != nil {
		t.Fatalf("StartCall failed: %v", err)
	}

	want := gatewayamqp.GatewayCallCommand{
		TenantID:    "tenant-1",
		ChannelID:   "channel-1",
		CommandID:   "cmd-1",
		CallID:      "CALL1",
		Action:      "video.orientation",
		To:          "+5511888888888",
		Targets:     []string{"a@s.whatsapp.net", "b@s.whatsapp.net"},
		GroupID:     "120363000000000000@g.us",
		Video:       true,
		MediaURL:    "https://example.test/clip.h264",
		Emoji:       "👍",
		Orientation: 3,
		Enabled:     true,
		Raised:      true,
		Participant: "c@s.whatsapp.net",
		LinkToken:   "TOK",
	}
	body, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("failed to marshal call command: %v", err)
	}

	pubCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open publish channel: %v", err)
	}
	t.Cleanup(func() {
		if err := pubCh.Close(); err != nil {
			t.Errorf("failed to close publish channel: %v", err)
		}
	})

	publishCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := pubCh.PublishWithContext(publishCtx, gatewayamqp.GatewayCallExchange, "0", false, false, rabbitmq.Publishing{
		ContentType: "application/json",
		Body:        body,
	}); err != nil {
		t.Fatalf("failed to publish call command: %v", err)
	}

	select {
	case got := <-received:
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("command =\n%+v\nwant\n%+v", got, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the call command")
	}
}

func TestPublisherPublishesCallEvent(t *testing.T) {
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
	if err := probeCh.QueueBind(q.Name, gatewayamqp.CallRoutingKey, gatewayamqp.EventsExchange, false, nil); err != nil {
		t.Fatalf("failed to bind probe queue: %v", err)
	}
	deliveries, err := probeCh.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to consume probe queue: %v", err)
	}

	evt := call.Event{
		PhoneNumberID: "5511999999999",
		TenantID:      "tenant-1",
		ChannelID:     "channel-1",
		CallID:        "CALL1",
		Direction:     call.DirectionInbound,
		Type:          call.EventEnded,
		Timestamp:     "1754300100",
		Duration:      10,
		Media:         &call.Media{Key: "calls/channel-1/CALL1.wav", MimeType: "audio/wav"},
	}
	publishCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := publisher.PublishCall(publishCtx, evt); err != nil {
		t.Fatalf("PublishCall failed: %v", err)
	}

	delivery := waitForDelivery(t, deliveries, gatewayamqp.CallRoutingKey, 10*time.Second)
	var got call.Event
	if err := json.Unmarshal(delivery.Body, &got); err != nil {
		t.Fatalf("failed to unmarshal call event: %v", err)
	}
	if !reflect.DeepEqual(got, evt) {
		t.Fatalf("event =\n%+v\nwant\n%+v", got, evt)
	}
}
