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
	"github.com/w3nder/whatsmeow-gateway/internal/ownership"
	"github.com/w3nder/whatsmeow-gateway/internal/registry"
	"github.com/w3nder/whatsmeow-gateway/internal/session"
)

// When RabbitMQ dies the gateway must not keep running with dead consumers: Run has to
// return the failure so main exits non-zero and the orchestrator restarts the process.
func TestGatewayRunFailsWhenRabbitMQDies(t *testing.T) {
	conn := startRabbitMQ(t)
	redisClient := startRedis(t)
	dsn := startPostgresForGateway(t)

	consumer, err := gatewayamqp.NewConsumer(conn, gatewayamqp.ConsumerConfig{Prefetch: 10})
	if err != nil {
		t.Fatalf("NewConsumer failed: %v", err)
	}
	publisher, err := gatewayamqp.NewPublisher(conn)
	if err != nil {
		t.Fatalf("NewPublisher failed: %v", err)
	}

	registryStore, err := registry.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("registry.Open failed: %v", err)
	}
	t.Cleanup(registryStore.Close)

	dedupeStore, err := dedupe.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("dedupe.Open failed: %v", err)
	}
	t.Cleanup(dedupeStore.Close)

	mgr := session.NewManager(func(_ string, _ *types.JID) (session.WAClient, error) {
		return newFakeWAClient(), nil
	})

	_, logger := logging.New()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- gateway.Run(ctx, gateway.Deps{
			Consumer:             consumer,
			Publisher:            publisher,
			Manager:              mgr,
			Ownership:            ownership.NewStore(redisClient, 4),
			Dedupe:               dedupeStore,
			Registry:             registryStore,
			MediaStore:           newUnusedMediaStore(t, "gateway-amqp-death-unused"),
			InstanceID:           "gateway-amqp-death-instance",
			ShardLockTTL:         30 * time.Second,
			ShutdownDrainTimeout: 5 * time.Second,
			Logger:               logger,
		})
	}()

	// Give Run time to start both consumers before the broker goes away.
	time.Sleep(2 * time.Second)

	if err := conn.Close(); err != nil {
		t.Fatalf("failed to close the rabbitmq connection: %v", err)
	}

	select {
	case runErr := <-runErrCh:
		if runErr == nil {
			t.Fatal("expected Run to return an error when rabbitmq died, so main exits non-zero and the process is restarted; got nil (the gateway would sit idle forever)")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not return after rabbitmq died: the gateway is a zombie")
	}
}

// A channel the boot resume never saw -- its row landed after startup, or the network
// was down when the gateway booted -- must come back from its stored JID on the next
// send. Building a fresh device instead would leave the channel unpaired and stuck
// waiting for a QR scan.
func TestGatewaySendResumesChannelMissedByBootResume(t *testing.T) {
	conn := startRabbitMQ(t)
	redisClient := startRedis(t)
	dsn := startPostgresForGateway(t)

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

	const messageID = "msg-late-resume-1"
	const channelID = "channel-late-resume-1"
	const tenantID = "tenant-late-resume-1"
	expectedProviderID := dedupe.DeterministicProviderID(messageID)
	storedJID := types.NewJID("15550007777", types.DefaultUserServer)

	fake := newFakeWAClient()
	fake.sendResp = whatsmeow.SendResponse{ID: expectedProviderID, Timestamp: time.Now().Truncate(time.Second)}

	factoryJIDs := make(chan *types.JID, 4)
	mgr := session.NewManager(func(_ string, jid *types.JID) (session.WAClient, error) {
		factoryJIDs <- jid
		return fake, nil
	})

	mediaStore := newUnusedMediaStore(t, "gateway-late-resume-unused")
	_, logger := logging.New()

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
		t.Fatalf("failed to bind probe queue: %v", err)
	}
	deliveries, err := probeCh.Consume(probeQ.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to consume probe queue: %v", err)
	}

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
			InstanceID:           "gateway-late-resume-instance",
			ShardLockTTL:         30 * time.Second,
			ShutdownDrainTimeout: 10 * time.Second,
			Logger:               logger,
		})
	}()

	// The row shows up only after boot, so resumeOwnedSessions never saw this channel.
	if err := registryStore.Save(context.Background(), channelID, storedJID.String(), tenantID); err != nil {
		t.Fatalf("registry.Save failed: %v", err)
	}

	sendCmd := gatewayamqp.GatewaySendCommand{
		TenantID:  tenantID,
		ChannelID: channelID,
		MessageID: messageID,
		To:        types.NewJID("15551234567", types.DefaultUserServer).String(),
		Type:      "text",
		Text:      "hello after a late resume",
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

	delivery := waitForDelivery(t, deliveries, gatewayamqp.StatusRoutingKey, 15*time.Second)
	var evt mapper.StatusEvent
	if err := json.Unmarshal(delivery.Body, &evt); err != nil {
		t.Fatalf("failed to unmarshal whatsapp.status.v1 event: %v", err)
	}
	if evt.Status != "sent" {
		t.Fatalf("expected the gateway to resume the channel from its stored jid and send, got %+v", evt)
	}

	select {
	case jid := <-factoryJIDs:
		if jid == nil {
			t.Fatal("expected the channel to be rebuilt from its stored jid, but a fresh unpaired device was created (that would force a re-pair)")
		}
		if *jid != storedJID {
			t.Fatalf("expected the device to be built for the stored jid %v, got %v", storedJID, *jid)
		}
	default:
		t.Fatal("expected the gateway to build a device for the channel")
	}

	if fake.qrChannelCallCount() != 0 {
		t.Fatalf("expected no QR flow when recovering a paired channel, got %d QRChannel calls", fake.qrChannelCallCount())
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
