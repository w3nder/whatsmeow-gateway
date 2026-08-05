package test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	rabbitmq "github.com/rabbitmq/amqp091-go"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	gatewayamqp "github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/gateway"
	"github.com/w3nder/whatsmeow-gateway/internal/logging"
	"github.com/w3nder/whatsmeow-gateway/internal/media"
	"github.com/w3nder/whatsmeow-gateway/internal/ownership"
	"github.com/w3nder/whatsmeow-gateway/internal/registry"
	"github.com/w3nder/whatsmeow-gateway/internal/session"
	"github.com/w3nder/whatsmeow-gateway/internal/store"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func newUnusedMediaStore(t *testing.T, bucket string) *media.S3Store {
	t.Helper()
	mediaStore, err := media.NewS3Store(context.Background(), media.S3Config{
		Bucket:          bucket,
		Region:          "us-east-1",
		Endpoint:        "http://127.0.0.1:1",
		AccessKeyID:     "unused",
		SecretAccessKey: "unused",
	})
	if err != nil {
		t.Fatalf("NewS3Store failed: %v", err)
	}
	return mediaStore
}

func TestGatewayBootResumesStoredSessionsWithoutQR(t *testing.T) {
	conn := startRabbitMQ(t)
	redisClient := startRedis(t)
	dsn := startPostgresForGateway(t)

	registryStore, err := registry.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("registry.Open failed: %v", err)
	}
	t.Cleanup(registryStore.Close)

	const channelID = "channel-restart-1"
	const tenantID = "tenant-restart-1"
	storedJID := types.NewJID("15551230000", types.DefaultUserServer)

	if err := registryStore.Save(context.Background(), channelID, storedJID.String(), tenantID); err != nil {
		t.Fatalf("registry.Save failed: %v", err)
	}

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

	fake := newFakeWAClient()
	var factoryCalledWithJID *types.JID
	mgr := session.NewManager(func(channelID string, jid *types.JID) (session.WAClient, error) {
		factoryCalledWithJID = jid
		return fake, nil
	})

	mediaStore := newUnusedMediaStore(t, "gateway-resume-unused")
	_, logger := logging.New()

	const instanceID = "gateway-resume-instance"
	ctx, cancel := context.WithCancel(context.Background())

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- gateway.Run(ctx, gateway.Deps{
			Consumer:             consumer,
			Publisher:            publisher,
			Manager:              mgr,
			Ownership:            ownershipStore,
			Registry:             registryStore,
			MediaStore:           mediaStore,
			InstanceID:           instanceID,
			ShardLockTTL:         30 * time.Second,
			ShutdownDrainTimeout: 10 * time.Second,
			Logger:               logger,
		})
	}()

	deadline := time.After(10 * time.Second)
	for fake.connectCallCount() == 0 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for the boot resume loop to call Connect on the stored session")
		case <-time.After(50 * time.Millisecond):
		}
	}

	if fake.connectCallCount() != 1 {
		t.Fatalf("expected Connect called exactly once during boot resume, got %d", fake.connectCallCount())
	}
	if fake.qrChannelCallCount() != 0 {
		t.Fatalf("expected no QR flow during boot resume, got %d QRChannel calls", fake.qrChannelCallCount())
	}
	if factoryCalledWithJID == nil || *factoryCalledWithJID != storedJID {
		t.Fatalf("expected the resume factory to be called with the stored jid %v, got %v", storedJID, factoryCalledWithJID)
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

func TestGatewayPersistsSessionOnPairSuccess(t *testing.T) {
	conn := startRabbitMQ(t)
	redisClient := startRedis(t)
	dsn := startPostgresForGateway(t)

	registryStore, err := registry.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("registry.Open failed: %v", err)
	}
	t.Cleanup(registryStore.Close)

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

	pairedJID := types.NewJID("15559990000", types.DefaultUserServer)
	fake := newFakeWAClient()
	fake.deviceJID = &pairedJID

	mgr := session.NewManager(func(channelID string, jid *types.JID) (session.WAClient, error) {
		return fake, nil
	})

	mediaStore := newUnusedMediaStore(t, "gateway-pair-persist-unused")
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
	if err := probeCh.QueueBind(probeQ.Name, gatewayamqp.ChannelStatusRoutingKey, gatewayamqp.EventsExchange, false, nil); err != nil {
		t.Fatalf("failed to bind probe queue: %v", err)
	}
	deliveries, err := probeCh.Consume(probeQ.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to consume probe queue: %v", err)
	}

	const instanceID = "gateway-pair-persist-instance"
	ctx, cancel := context.WithCancel(context.Background())

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- gateway.Run(ctx, gateway.Deps{
			Consumer:             consumer,
			Publisher:            publisher,
			Manager:              mgr,
			Ownership:            ownershipStore,
			Registry:             registryStore,
			MediaStore:           mediaStore,
			InstanceID:           instanceID,
			ShardLockTTL:         30 * time.Second,
			ShutdownDrainTimeout: 10 * time.Second,
			Logger:               logger,
		})
	}()

	const channelID = "channel-pair-persist-1"
	const tenantID = "tenant-pair-persist-1"

	pairCmd := gatewayamqp.PairCommand{TenantID: tenantID, ChannelID: channelID, UserID: "user-pair-persist-1"}
	pairBody, err := json.Marshal(pairCmd)
	if err != nil {
		t.Fatalf("failed to marshal pair command: %v", err)
	}
	publishCtx, publishCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer publishCancel()
	if err := probeCh.PublishWithContext(publishCtx, gatewayamqp.GatewayPairExchange, "0", false, false, rabbitmq.Publishing{
		ContentType: "application/json",
		Body:        pairBody,
	}); err != nil {
		t.Fatalf("failed to publish pair command: %v", err)
	}

	statusDelivery := waitForDelivery(t, deliveries, gatewayamqp.ChannelStatusRoutingKey, 10*time.Second)
	var statusEvt gatewayamqp.ChannelStatusEvent
	if err := json.Unmarshal(statusDelivery.Body, &statusEvt); err != nil {
		t.Fatalf("failed to unmarshal channel.status event: %v", err)
	}
	if statusEvt.Status != "connected" {
		t.Fatalf("unexpected channel.status event: %+v", statusEvt)
	}

	sessions, err := registryStore.ForShards(context.Background(), []int{0, 1, 2, 3}, func(cid string) int {
		return ownership.Shard(cid, shardCount)
	})
	if err != nil {
		t.Fatalf("ForShards failed: %v", err)
	}

	var found *registry.ChannelSession
	for i := range sessions {
		if sessions[i].ChannelID == channelID {
			found = &sessions[i]
		}
	}
	if found == nil {
		t.Fatalf("expected pair-success to persist a session for %s, got %+v", channelID, sessions)
	}
	if found.JID != pairedJID.String() {
		t.Fatalf("expected persisted jid %s, got %s", pairedJID.String(), found.JID)
	}
	if found.TenantID != tenantID {
		t.Fatalf("expected persisted tenant %s, got %s", tenantID, found.TenantID)
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

func TestGatewayBootSkipsStaleDeviceRowAndResumesValidChannel(t *testing.T) {
	conn := startRabbitMQ(t)
	redisClient := startRedis(t)
	dsn := startPostgresForGateway(t)

	registryStore, err := registry.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("registry.Open failed: %v", err)
	}
	t.Cleanup(registryStore.Close)

	waLogger, waSlogger := logging.New()
	sessionContainer, err := store.Open(context.Background(), dsn, waLogger)
	if err != nil {
		t.Fatalf("store.Open failed: %v", err)
	}

	const staleChannelID = "channel-stale-device-1"
	const staleTenantID = "tenant-stale-1"
	staleJID := types.NewJID("15550001111", types.DefaultUserServer)
	if err := registryStore.Save(context.Background(), staleChannelID, staleJID.String(), staleTenantID); err != nil {
		t.Fatalf("registry.Save(stale) failed: %v", err)
	}

	const validChannelID = "channel-valid-resume-1"
	const validTenantID = "tenant-valid-1"
	validJID := types.NewJID("15550002222", types.DefaultUserServer)
	if err := registryStore.Save(context.Background(), validChannelID, validJID.String(), validTenantID); err != nil {
		t.Fatalf("registry.Save(valid) failed: %v", err)
	}

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

	realFactory := gateway.NewWAClientFactory(sessionContainer, waLogger, waSlogger)
	fake := newFakeWAClient()

	mgr := session.NewManager(func(channelID string, jid *types.JID) (session.WAClient, error) {
		if channelID == validChannelID {
			return fake, nil
		}
		return realFactory(channelID, jid)
	})

	mediaStore := newUnusedMediaStore(t, "gateway-stale-device-unused")

	logBuf := &syncBuffer{}
	logger := slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	const instanceID = "gateway-stale-device-instance"
	ctx, cancel := context.WithCancel(context.Background())

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- gateway.Run(ctx, gateway.Deps{
			Consumer:             consumer,
			Publisher:            publisher,
			Manager:              mgr,
			Ownership:            ownershipStore,
			Registry:             registryStore,
			MediaStore:           mediaStore,
			InstanceID:           instanceID,
			ShardLockTTL:         30 * time.Second,
			ShutdownDrainTimeout: 10 * time.Second,
			Logger:               logger,
		})
	}()

	deadline := time.After(10 * time.Second)
	for fake.connectCallCount() == 0 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for the boot resume loop to resume the valid channel past the stale one")
		case <-time.After(50 * time.Millisecond):
		}
	}

	if fake.connectCallCount() != 1 {
		t.Fatalf("expected the valid channel's Connect called exactly once, got %d", fake.connectCallCount())
	}
	if fake.qrChannelCallCount() != 0 {
		t.Fatalf("expected no QR flow during boot resume, got %d QRChannel calls", fake.qrChannelCallCount())
	}

	logs := logBuf.String()
	if !strings.Contains(logs, staleChannelID) {
		t.Fatalf("expected the stale channel's resume failure to be logged, got logs: %s", logs)
	}
	if !strings.Contains(logs, "resume session") {
		t.Fatalf("expected a %q log message for the stale channel, got logs: %s", "resume session", logs)
	}

	cancel()

	select {
	case runErr := <-runErrCh:
		if runErr != nil {
			t.Fatalf("gateway.Run returned an error on shutdown: %v", runErr)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("gateway.Run did not return after ctx cancellation (boot resume must never panic/crash on a stale device row)")
	}
}

func TestGatewayDeletesSessionFromRegistryOnLoggedOut(t *testing.T) {
	conn := startRabbitMQ(t)
	redisClient := startRedis(t)
	dsn := startPostgresForGateway(t)

	registryStore, err := registry.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("registry.Open failed: %v", err)
	}
	t.Cleanup(registryStore.Close)

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

	pairedJID := types.NewJID("15559991111", types.DefaultUserServer)
	fake := newFakeWAClient()
	fake.deviceJID = &pairedJID

	mgr := session.NewManager(func(channelID string, jid *types.JID) (session.WAClient, error) {
		return fake, nil
	})

	mediaStore := newUnusedMediaStore(t, "gateway-loggedout-unused")
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
	if err := probeCh.QueueBind(probeQ.Name, gatewayamqp.ChannelStatusRoutingKey, gatewayamqp.EventsExchange, false, nil); err != nil {
		t.Fatalf("failed to bind probe queue: %v", err)
	}
	deliveries, err := probeCh.Consume(probeQ.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to consume probe queue: %v", err)
	}

	const instanceID = "gateway-loggedout-instance"
	ctx, cancel := context.WithCancel(context.Background())

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- gateway.Run(ctx, gateway.Deps{
			Consumer:             consumer,
			Publisher:            publisher,
			Manager:              mgr,
			Ownership:            ownershipStore,
			Registry:             registryStore,
			MediaStore:           mediaStore,
			InstanceID:           instanceID,
			ShardLockTTL:         30 * time.Second,
			ShutdownDrainTimeout: 10 * time.Second,
			Logger:               logger,
		})
	}()

	const channelID = "channel-loggedout-1"
	const tenantID = "tenant-loggedout-1"

	pairCmd := gatewayamqp.PairCommand{TenantID: tenantID, ChannelID: channelID, UserID: "user-loggedout-1"}
	pairBody, err := json.Marshal(pairCmd)
	if err != nil {
		t.Fatalf("failed to marshal pair command: %v", err)
	}
	publishCtx, publishCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer publishCancel()
	if err := probeCh.PublishWithContext(publishCtx, gatewayamqp.GatewayPairExchange, "0", false, false, rabbitmq.Publishing{
		ContentType: "application/json",
		Body:        pairBody,
	}); err != nil {
		t.Fatalf("failed to publish pair command: %v", err)
	}

	waitForDelivery(t, deliveries, gatewayamqp.ChannelStatusRoutingKey, 10*time.Second)

	sessions, err := registryStore.ForShards(context.Background(), []int{0, 1, 2, 3}, func(cid string) int {
		return ownership.Shard(cid, shardCount)
	})
	if err != nil {
		t.Fatalf("ForShards (before logout) failed: %v", err)
	}
	found := false
	for _, cs := range sessions {
		if cs.ChannelID == channelID {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected the session to be persisted before logout, got %+v", sessions)
	}

	fake.emit(&events.LoggedOut{})

	deadline := time.After(5 * time.Second)
	for {
		sessions, err := registryStore.ForShards(context.Background(), []int{0, 1, 2, 3}, func(cid string) int {
			return ownership.Shard(cid, shardCount)
		})
		if err != nil {
			t.Fatalf("ForShards (after logout) failed: %v", err)
		}
		stillPresent := false
		for _, cs := range sessions {
			if cs.ChannelID == channelID {
				stillPresent = true
			}
		}
		if !stillPresent {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for the LoggedOut event to delete the registry row for %s", channelID)
		case <-time.After(50 * time.Millisecond):
		}
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
