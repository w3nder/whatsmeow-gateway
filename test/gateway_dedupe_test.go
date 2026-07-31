package test

import (
	"context"
	"encoding/json"
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

func TestGatewaySendHandlerDedupesRedeliveredCommand(t *testing.T) {
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

	const messageID = "msg-dedupe-1"
	expectedProviderID := dedupe.DeterministicProviderID(messageID)

	fake := newFakeWAClient()
	fake.sendResp = whatsmeow.SendResponse{ID: expectedProviderID, Timestamp: time.Now().Truncate(time.Second)}

	mgr := session.NewManager(func(channelID string, jid *types.JID) (session.WAClient, error) {
		return fake, nil
	})

	mediaStore, err := media.NewS3Store(context.Background(), media.S3Config{
		Bucket:          "gateway-dedupe-unused",
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
	dedupeStore, err := dedupe.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("dedupe.Open failed: %v", err)
	}
	t.Cleanup(dedupeStore.Close)

	registryStore, err := registry.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("registry.Open failed: %v", err)
	}
	t.Cleanup(registryStore.Close)

	// A send only reaches a paired channel: the gateway resumes it from the stored JID.
	storedJID := types.NewJID("15550008888", types.DefaultUserServer)
	if err := registryStore.Save(context.Background(), "channel-dedupe-1", storedJID.String(), "tenant-dedupe-1"); err != nil {
		t.Fatalf("registry.Save failed: %v", err)
	}

	probeCh, err := conn.Channel()
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
	deliveries, err := probeCh.Consume(probeQ.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to consume probe queue: %v", err)
	}

	const instanceID = "gateway-dedupe-instance"
	ctx, cancel := context.WithCancel(context.Background())

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- gateway.Run(ctx, gateway.Deps{
			Consumer:             consumer,
			Publisher:            publisher,
			Manager:              mgr,
			Ownership:            ownershipStore,
			Dedupe:               dedupeStore,
			Registry:             registryStore,
			MediaStore:           mediaStore,
			InstanceID:           instanceID,
			ShardLockTTL:         30 * time.Second,
			ShutdownDrainTimeout: 10 * time.Second,
			Logger:               logger,
		})
	}()

	sendCmd := gatewayamqp.GatewaySendCommand{
		TenantID:  "tenant-dedupe-1",
		ChannelID: "channel-dedupe-1",
		MessageID: messageID,
		To:        "15551234567@s.whatsapp.net",
		Type:      "text",
		Text:      "hello from dedupe test",
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
		t.Fatalf("failed to publish first send command: %v", err)
	}

	firstDelivery := waitForDelivery(t, deliveries, gatewayamqp.StatusRoutingKey, 10*time.Second)
	var firstEvt mapper.StatusEvent
	if err := json.Unmarshal(firstDelivery.Body, &firstEvt); err != nil {
		t.Fatalf("failed to unmarshal first whatsapp.status.v1 event: %v", err)
	}
	if firstEvt.Status != "sent" || firstEvt.ProviderMessageID != expectedProviderID {
		t.Fatalf("unexpected first whatsapp.status.v1 event: %+v (want providerMessageId=%s)", firstEvt, expectedProviderID)
	}
	if fake.sendCallCount() != 1 {
		t.Fatalf("expected WAClient.SendMessage called once after the first delivery, got %d", fake.sendCallCount())
	}
	if fake.lastID != expectedProviderID {
		t.Fatalf("expected the first send to use the deterministic id %q, got %q", expectedProviderID, fake.lastID)
	}

	if err := probeCh.PublishWithContext(publishCtx, gatewayamqp.GatewaySendExchange, "0", false, false, rabbitmq.Publishing{
		ContentType: "application/json",
		Body:        sendBody,
	}); err != nil {
		t.Fatalf("failed to publish redelivered send command: %v", err)
	}

	secondDelivery := waitForDelivery(t, deliveries, gatewayamqp.StatusRoutingKey, 10*time.Second)
	var secondEvt mapper.StatusEvent
	if err := json.Unmarshal(secondDelivery.Body, &secondEvt); err != nil {
		t.Fatalf("failed to unmarshal second whatsapp.status.v1 event: %v", err)
	}
	if secondEvt.Status != "sent" || secondEvt.ProviderMessageID != expectedProviderID {
		t.Fatalf("unexpected second whatsapp.status.v1 event: %+v (want providerMessageId=%s)", secondEvt, expectedProviderID)
	}

	if fake.sendCallCount() != 1 {
		t.Fatalf("expected WAClient.SendMessage still called exactly once after the redelivered command, got %d", fake.sendCallCount())
	}

	alreadySent, existingProviderID, err := dedupeStore.Begin(context.Background(), messageID, expectedProviderID)
	if err != nil {
		t.Fatalf("Begin (verification) failed: %v", err)
	}
	if !alreadySent {
		t.Fatal("expected the ledger row to be status='sent' after the redelivered command was deduped")
	}
	if existingProviderID != expectedProviderID {
		t.Fatalf("expected the ledger's provider_message_id to be %q, got %q", expectedProviderID, existingProviderID)
	}

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
