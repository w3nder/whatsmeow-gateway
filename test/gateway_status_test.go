package test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	rabbitmq "github.com/rabbitmq/amqp091-go"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"

	gatewayamqp "github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/dedupe"
	"github.com/w3nder/whatsmeow-gateway/internal/gateway"
	"github.com/w3nder/whatsmeow-gateway/internal/logging"
	"github.com/w3nder/whatsmeow-gateway/internal/mapper"
	"github.com/w3nder/whatsmeow-gateway/internal/media"
	"github.com/w3nder/whatsmeow-gateway/internal/ownership"
	"github.com/w3nder/whatsmeow-gateway/internal/registry"
	"github.com/w3nder/whatsmeow-gateway/internal/session"
)

func setupStatusRoundtripGateway(t *testing.T, fake *fakeWAClient, channelID string) (probeCh *rabbitmq.Channel, deliveries <-chan rabbitmq.Delivery, dedupeStore *dedupe.Store, cancel context.CancelFunc, runErrCh chan error) {
	t.Helper()

	conn := startRabbitMQ(t)
	redisClient := startRedis(t)

	consumer, err := gatewayamqp.NewConsumer(conn, gatewayamqp.ConsumerConfig{Prefetch: 10})
	if err != nil {
		t.Fatalf("NewConsumer failed: %v", err)
	}

	publisher, err := gatewayamqp.NewPublisher(conn)
	if err != nil {
		t.Fatalf("NewPublisher failed: %v", err)
	}
	t.Cleanup(func() {
		if err := publisher.Close(); err != nil {
			t.Errorf("publisher.Close failed: %v", err)
		}
	})

	const shardCount = 4
	ownershipStore := ownership.NewStore(redisClient, shardCount)

	mgr := session.NewManager(func(channelID string, jid *types.JID) (session.WAClient, error) {
		return fake, nil
	})

	mediaStore, err := media.NewS3Store(context.Background(), media.S3Config{
		Bucket:          "gateway-status-roundtrip-unused",
		Region:          "us-east-1",
		Endpoint:        "http://127.0.0.1:1",
		AccessKeyID:     "unused",
		SecretAccessKey: "unused",
	})
	if err != nil {
		t.Fatalf("NewS3Store failed: %v", err)
	}

	_, logger := logging.New()

	dsn := startPostgresForGateway(t)
	dedupeStore, err = dedupe.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("dedupe.Open failed: %v", err)
	}
	t.Cleanup(dedupeStore.Close)

	registryStore, err := registry.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("registry.Open failed: %v", err)
	}
	t.Cleanup(registryStore.Close)

	storedJID := types.NewJID("15550009999", types.DefaultUserServer)
	if err := registryStore.Save(context.Background(), channelID, storedJID.String(), "tenant-status-roundtrip"); err != nil {
		t.Fatalf("registry.Save failed: %v", err)
	}

	probeCh, err = conn.Channel()
	if err != nil {
		t.Fatalf("failed to open probe channel: %v", err)
	}
	t.Cleanup(func() {
		if err := probeCh.Close(); err != nil {
			t.Errorf("failed to close probe channel: %v", err)
		}
	})

	probeQ, err := probeCh.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatalf("failed to declare probe queue: %v", err)
	}
	if err := probeCh.QueueBind(probeQ.Name, gatewayamqp.StatusRoutingKey, gatewayamqp.EventsExchange, false, nil); err != nil {
		t.Fatalf("failed to bind probe queue to %s: %v", gatewayamqp.StatusRoutingKey, err)
	}
	deliveries, err = probeCh.Consume(probeQ.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to consume probe queue: %v", err)
	}

	var ctx context.Context
	ctx, cancel = context.WithCancel(context.Background())

	runErrCh = make(chan error, 1)
	go func() {
		runErrCh <- gateway.Run(ctx, gateway.Deps{
			Consumer:             consumer,
			Publisher:            publisher,
			Manager:              mgr,
			Ownership:            ownershipStore,
			Dedupe:               dedupeStore,
			Registry:             registryStore,
			MediaStore:           mediaStore,
			InstanceID:           "gateway-status-roundtrip-instance",
			ShardLockTTL:         30 * time.Second,
			ShutdownDrainTimeout: 10 * time.Second,
			Logger:               logger,
		})
	}()

	return probeCh, deliveries, dedupeStore, cancel, runErrCh
}

func shutdownStatusRoundtripGateway(t *testing.T, cancel context.CancelFunc, runErrCh chan error) {
	t.Helper()
	cancel()
	select {
	case runErr := <-runErrCh:
		if runErr != nil {
			t.Fatalf("gateway.Run returned an error on shutdown: %v", runErr)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("gateway.Run did not return after ctx cancellation")
	}
}

func TestGatewaySendHandlerPublishesOpaqueMessageIdOnSent(t *testing.T) {
	const messageID = "msg-status-roundtrip-sent-1"
	expectedProviderID := dedupe.DeterministicProviderID(messageID)

	fake := newFakeWAClient()
	fake.sendResp = whatsmeow.SendResponse{ID: expectedProviderID, Timestamp: time.Now().Truncate(time.Second)}

	probeCh, deliveries, _, cancel, runErrCh := setupStatusRoundtripGateway(t, fake, "channel-status-roundtrip-1")

	sendCmd := gatewayamqp.GatewaySendCommand{
		TenantID:  "tenant-status-roundtrip-1",
		ChannelID: "channel-status-roundtrip-1",
		MessageID: messageID,
		To:        "15551234567@s.whatsapp.net",
		Type:      "text",
		Text:      "hello from status roundtrip",
	}
	sendBody, err := json.Marshal(sendCmd)
	if err != nil {
		t.Fatalf("failed to marshal send command: %v", err)
	}

	publishCtx, publishCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer publishCancel()
	if err := probeCh.PublishWithContext(publishCtx, gatewayamqp.GatewaySendExchange, "0", false, false, rabbitmq.Publishing{
		ContentType: "application/json",
		Body:        sendBody,
	}); err != nil {
		t.Fatalf("failed to publish send command: %v", err)
	}

	delivery := waitForDelivery(t, deliveries, gatewayamqp.StatusRoutingKey, 10*time.Second)
	var evt mapper.StatusEvent
	if err := json.Unmarshal(delivery.Body, &evt); err != nil {
		t.Fatalf("failed to unmarshal whatsapp.status.v1 event: %v", err)
	}
	if evt.Status != "sent" {
		t.Fatalf("expected status=sent, got %+v", evt)
	}
	if evt.ProviderMessageID != expectedProviderID {
		t.Fatalf("expected providerMessageId=%s, got %+v", expectedProviderID, evt)
	}
	if evt.OpaqueMessageID != messageID {
		t.Fatalf("expected opaqueMessageId=%s (the business Message id) so ingestion can backfill providerMessageId, got %+v", messageID, evt)
	}
	if evt.Error != nil {
		t.Fatalf("expected no error on a successful send, got %+v", evt.Error)
	}

	shutdownStatusRoundtripGateway(t, cancel, runErrCh)
}

func TestGatewaySendHandlerPublishesFailedStatusAndAcksOnSendError(t *testing.T) {
	const messageID = "msg-status-roundtrip-failed-1"
	expectedProviderID := dedupe.DeterministicProviderID(messageID)

	fake := newFakeWAClient()
	fake.sendErr = errors.New("boom: whatsapp rejected the message")

	probeCh, deliveries, dedupeStore, cancel, runErrCh := setupStatusRoundtripGateway(t, fake, "channel-status-roundtrip-2")

	dlqCh, err := probeCh.Consume(gatewayamqp.GatewaySendDLQ, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to consume %s: %v", gatewayamqp.GatewaySendDLQ, err)
	}

	sendCmd := gatewayamqp.GatewaySendCommand{
		TenantID:  "tenant-status-roundtrip-2",
		ChannelID: "channel-status-roundtrip-2",
		MessageID: messageID,
		To:        "15557654321@s.whatsapp.net",
		Type:      "text",
		Text:      "this send will fail",
	}
	sendBody, err := json.Marshal(sendCmd)
	if err != nil {
		t.Fatalf("failed to marshal send command: %v", err)
	}

	publishCtx, publishCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer publishCancel()
	if err := probeCh.PublishWithContext(publishCtx, gatewayamqp.GatewaySendExchange, "0", false, false, rabbitmq.Publishing{
		ContentType: "application/json",
		Body:        sendBody,
	}); err != nil {
		t.Fatalf("failed to publish send command: %v", err)
	}

	delivery := waitForDelivery(t, deliveries, gatewayamqp.StatusRoutingKey, 10*time.Second)
	var evt mapper.StatusEvent
	if err := json.Unmarshal(delivery.Body, &evt); err != nil {
		t.Fatalf("failed to unmarshal whatsapp.status.v1 event: %v", err)
	}
	if evt.Status != "failed" {
		t.Fatalf("expected status=failed, got %+v", evt)
	}
	if evt.ProviderMessageID != expectedProviderID {
		t.Fatalf("expected providerMessageId=%s, got %+v", expectedProviderID, evt)
	}
	if evt.OpaqueMessageID != messageID {
		t.Fatalf("expected opaqueMessageId=%s so ingestion can mark the right Message failed, got %+v", messageID, evt)
	}
	if evt.Error == nil || evt.Error.Reason == "" {
		t.Fatalf("expected a populated error reason on a failed send, got %+v", evt)
	}

	select {
	case d := <-dlqCh:
		t.Fatalf("expected the failed send command to be acked (status published instead), got it dead-lettered: %s", d.Body)
	case <-time.After(3 * time.Second):
	}

	if fake.sendCallCount() != 1 {
		t.Fatalf("expected WAClient.SendMessage called exactly once, got %d", fake.sendCallCount())
	}

	alreadySent, existingProviderID, err := dedupeStore.Begin(context.Background(), messageID, expectedProviderID)
	if err != nil {
		t.Fatalf("Begin (verification) failed: %v", err)
	}
	if alreadySent {
		t.Fatal("expected the ledger row to remain pending (not sent) after a failed send")
	}
	if existingProviderID != expectedProviderID {
		t.Fatalf("expected the ledger's provider_message_id to be %q, got %q", expectedProviderID, existingProviderID)
	}

	shutdownStatusRoundtripGateway(t, cancel, runErrCh)
}
