package test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	rabbitmq "github.com/rabbitmq/amqp091-go"

	gatewayamqp "github.com/w3nder/whatsmeow-gateway/internal/amqp"
)

func TestConsumerHandlesGatewayGroupCommand(t *testing.T) {
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

	received := make(chan gatewayamqp.GatewayGroupCommand, 1)
	if err := consumer.StartGroup(context.Background(), func(_ context.Context, cmd gatewayamqp.GatewayGroupCommand) error {
		received <- cmd
		return nil
	}); err != nil {
		t.Fatalf("StartGroup failed: %v", err)
	}

	publishCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("open publish channel: %v", err)
	}
	t.Cleanup(func() { _ = publishCh.Close() })

	cmd := gatewayamqp.GatewayGroupCommand{
		CommandID: "cmd-1",
		TenantID:  "tenant-1",
		ChannelID: "channel-1",
		Action:    "lock",
		GroupJIDs: []string{"120363422547615282@g.us"},
	}
	body, _ := json.Marshal(cmd)
	if err := publishCh.PublishWithContext(context.Background(), gatewayamqp.GatewayGroupExchange, "17", false, false, rabbitmq.Publishing{
		ContentType: "application/json", DeliveryMode: rabbitmq.Persistent, Body: body,
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case got := <-received:
		if got.CommandID != "cmd-1" || got.Action != "lock" || len(got.GroupJIDs) != 1 {
			t.Fatalf("handler got %+v", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the group command")
	}
}

func TestPublisherPublishesGroupEvents(t *testing.T) {
	conn := startRabbitMQ(t)

	publisher, err := gatewayamqp.NewPublisher(conn)
	if err != nil {
		t.Fatalf("NewPublisher failed: %v", err)
	}
	t.Cleanup(func() { _ = publisher.Close() })

	probeCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("open probe channel: %v", err)
	}
	t.Cleanup(func() { _ = probeCh.Close() })
	q, err := probeCh.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatalf("declare probe queue: %v", err)
	}
	for _, rk := range []string{gatewayamqp.GroupActionRoutingKey, gatewayamqp.GroupParticipantsRoutingKey} {
		if err := probeCh.QueueBind(q.Name, rk, gatewayamqp.EventsExchange, false, nil); err != nil {
			t.Fatalf("bind %s: %v", rk, err)
		}
	}
	deliveries, err := probeCh.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume probe: %v", err)
	}

	ctx := context.Background()
	if err := publisher.PublishGroupAction(ctx, gatewayamqp.GroupActionEvent{TenantID: "t", ChannelID: "c", CommandID: "cmd-1", GroupJID: "g@g.us", Action: "lock", OK: true}); err != nil {
		t.Fatalf("PublishGroupAction: %v", err)
	}
	if err := publisher.PublishGroupParticipants(ctx, gatewayamqp.GroupParticipantsEvent{TenantID: "t", ChannelID: "c", GroupJID: "g@g.us", Type: "join", EventID: "e1", OccurredAt: "2026-09-24T12:00:00Z"}); err != nil {
		t.Fatalf("PublishGroupParticipants: %v", err)
	}

	action := waitForDelivery(t, deliveries, gatewayamqp.GroupActionRoutingKey, 10*time.Second)
	var got gatewayamqp.GroupActionEvent
	if err := json.Unmarshal(action.Body, &got); err != nil || got.CommandID != "cmd-1" || !got.OK {
		t.Fatalf("group action on the wire: %s (%v)", action.Body, err)
	}
	participants := waitForDelivery(t, deliveries, gatewayamqp.GroupParticipantsRoutingKey, 10*time.Second)
	var gotP gatewayamqp.GroupParticipantsEvent
	if err := json.Unmarshal(participants.Body, &gotP); err != nil || gotP.EventID != "e1" || gotP.Participants == nil {
		t.Fatalf("participants on the wire: %s (%v)", participants.Body, err)
	}
}
