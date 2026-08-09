package test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

// TestConsumerAdmitsPairCommandsWhileASessionIsInFlight: a pairing waits on a person,
// so it holds its handler for the whole minute the operator has to scan. Consuming pair
// commands one at a time meant the next one -- the same operator reopening the dialog,
// or an entirely different channel -- sat unread on the queue for that minute. The
// close at the end pins the other half: a session admitted this way is still waited for
// on the way down rather than abandoned mid-pairing.
func TestConsumerAdmitsPairCommandsWhileASessionIsInFlight(t *testing.T) {
	conn := startRabbitMQ(t)

	consumer, err := gatewayamqp.NewConsumer(conn, gatewayamqp.ConsumerConfig{Prefetch: 10})
	if err != nil {
		t.Fatalf("NewConsumer failed: %v", err)
	}

	admitted := make(chan string, 2)
	release := make(chan struct{})
	var finished int32
	err = consumer.StartPair(context.Background(), func(_ context.Context, cmd gatewayamqp.PairCommand, accept func()) error {
		accept()
		admitted <- cmd.ChannelID
		// Stands in for the QR loop: nothing more happens until the operator acts.
		<-release
		atomic.AddInt32(&finished, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("StartPair failed: %v", err)
	}

	publishCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open publish channel: %v", err)
	}
	t.Cleanup(func() {
		if err := publishCh.Close(); err != nil && !errors.Is(err, rabbitmq.ErrClosed) {
			t.Errorf("failed to close publish channel: %v", err)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	channels := []string{"channel-pair-inflight-1", "channel-pair-inflight-2"}
	for _, channelID := range channels {
		body, err := json.Marshal(gatewayamqp.PairCommand{TenantID: "tenant-1", ChannelID: channelID, UserID: "user-1"})
		if err != nil {
			t.Fatalf("failed to marshal pair command: %v", err)
		}
		if err := publishCh.PublishWithContext(ctx, gatewayamqp.GatewayPairExchange, "0", false, false, rabbitmq.Publishing{
			ContentType:  "application/json",
			DeliveryMode: rabbitmq.Persistent,
			Body:         body,
		}); err != nil {
			t.Fatalf("failed to publish pair command for %s: %v", channelID, err)
		}
	}

	seen := make(map[string]bool, len(channels))
	for range channels {
		select {
		case channelID := <-admitted:
			seen[channelID] = true
		// Generous on purpose: a command held behind the session ahead of it is not
		// admitted late, it is never admitted, so the slack cannot mask the bug.
		case <-time.After(30 * time.Second):
			t.Fatalf("only %v was admitted: a pair command must not wait out the pairing session ahead of it", seen)
		}
	}
	for _, channelID := range channels {
		if !seen[channelID] {
			t.Fatalf("expected both pair commands to be admitted, %s never reached the handler", channelID)
		}
	}

	closed := make(chan error, 1)
	go func() { closed <- consumer.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned while two pairings were still running (err %v): an in-flight pairing must not be silently abandoned", err)
	case <-time.After(500 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return after the pairings ended")
	}
	if got := atomic.LoadInt32(&finished); got != int32(len(channels)) {
		t.Fatalf("expected both pairing sessions to run to their end before Close returned, got %d", got)
	}
}

// TestConsumerAdmitsManyPairCommandsConcurrently pins that nothing here caps how many
// pairings run at once. It publishes more channels than the old fixed cap ever allowed
// concurrently and holds every one of them open on its own release gate; if a cap were
// still in place, the channels past it would never be admitted and this test would time
// out waiting for them.
func TestConsumerAdmitsManyPairCommandsConcurrently(t *testing.T) {
	const channelCount = 40

	conn := startRabbitMQ(t)

	consumer, err := gatewayamqp.NewConsumer(conn, gatewayamqp.ConsumerConfig{Prefetch: channelCount})
	if err != nil {
		t.Fatalf("NewConsumer failed: %v", err)
	}

	admitted := make(chan string, channelCount)
	release := make(chan struct{})
	var finished int32
	err = consumer.StartPair(context.Background(), func(_ context.Context, cmd gatewayamqp.PairCommand, accept func()) error {
		accept()
		admitted <- cmd.ChannelID
		// Every session parks here until the test lets them all go, so none can finish
		// early and free up room for one still waiting to be admitted.
		<-release
		atomic.AddInt32(&finished, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("StartPair failed: %v", err)
	}

	publishCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open publish channel: %v", err)
	}
	t.Cleanup(func() {
		if err := publishCh.Close(); err != nil && !errors.Is(err, rabbitmq.ErrClosed) {
			t.Errorf("failed to close publish channel: %v", err)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	channels := make([]string, channelCount)
	for i := range channels {
		channels[i] = fmt.Sprintf("channel-pair-concurrent-%d", i)
		body, err := json.Marshal(gatewayamqp.PairCommand{TenantID: "tenant-1", ChannelID: channels[i], UserID: "user-1"})
		if err != nil {
			t.Fatalf("failed to marshal pair command: %v", err)
		}
		if err := publishCh.PublishWithContext(ctx, gatewayamqp.GatewayPairExchange, "0", false, false, rabbitmq.Publishing{
			ContentType:  "application/json",
			DeliveryMode: rabbitmq.Persistent,
			Body:         body,
		}); err != nil {
			t.Fatalf("failed to publish pair command for %s: %v", channels[i], err)
		}
	}

	seen := make(map[string]bool, len(channels))
	for range channels {
		select {
		case channelID := <-admitted:
			seen[channelID] = true
		case <-time.After(30 * time.Second):
			t.Fatalf("only %d of %d channels were admitted: a fixed cap would strand the rest behind sessions that have not finished", len(seen), channelCount)
		}
	}
	for _, channelID := range channels {
		if !seen[channelID] {
			t.Fatalf("expected every channel to be admitted, %s never reached the handler", channelID)
		}
	}

	close(release)
	deadline := time.After(10 * time.Second)
	for atomic.LoadInt32(&finished) != int32(channelCount) {
		select {
		case <-deadline:
			t.Fatalf("expected all %d sessions to finish, got %d", channelCount, atomic.LoadInt32(&finished))
		case <-time.After(10 * time.Millisecond):
		}
	}

	if err := consumer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
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
