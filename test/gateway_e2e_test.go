package test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	rabbitmq "github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	gatewayamqp "github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/gateway"
	"github.com/w3nder/whatsmeow-gateway/internal/logging"
	"github.com/w3nder/whatsmeow-gateway/internal/mapper"
	"github.com/w3nder/whatsmeow-gateway/internal/media"
	"github.com/w3nder/whatsmeow-gateway/internal/ownership"
	"github.com/w3nder/whatsmeow-gateway/internal/session"
	"github.com/w3nder/whatsmeow-gateway/internal/store"
)

func startPostgresForGateway(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	pgContainer, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("whatsmeow"),
		tcpostgres.WithUsername("whatsmeow"),
		tcpostgres.WithPassword("whatsmeow"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}
	t.Cleanup(func() {
		if err := pgContainer.Terminate(context.Background()); err != nil {
			t.Errorf("failed to terminate postgres container: %v", err)
		}
	})

	dsn, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}
	return dsn
}

func waitForDelivery(t *testing.T, deliveries <-chan rabbitmq.Delivery, wantRoutingKey string, timeout time.Duration) rabbitmq.Delivery {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case d := <-deliveries:
			if d.RoutingKey == wantRoutingKey {
				return d
			}
		case <-deadline:
			t.Fatalf("timed out waiting for a delivery with routing key %q", wantRoutingKey)
		}
	}
}

func TestGatewayPartialBootFailureClosesConsumerAndReleasesShards(t *testing.T) {
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

	deleteCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open delete channel: %v", err)
	}
	t.Cleanup(func() {
		if err := deleteCh.Close(); err != nil && err != rabbitmq.ErrClosed {
			t.Errorf("failed to close delete channel: %v", err)
		}
	})
	if _, err := deleteCh.QueueDelete(gatewayamqp.GatewaySendQueue, false, false, false); err != nil {
		t.Fatalf("failed to delete %s to force a StartSend failure: %v", gatewayamqp.GatewaySendQueue, err)
	}

	const shardCount = 4
	ownershipStore := ownership.NewStore(redisClient, shardCount)

	fake := newFakeWAClient()
	fake.qrItems = []whatsmeow.QRChannelItem{
		{Event: "code", Code: "leaked-consumer-qr"},
		whatsmeow.QRChannelSuccess,
	}
	mgr := session.NewManager(func(channelID string) (session.WAClient, error) {
		return fake, nil
	})

	mediaStore, err := media.NewS3Store(context.Background(), media.S3Config{
		Bucket:          "gateway-partial-boot-unused",
		Region:          "us-east-1",
		Endpoint:        "http://127.0.0.1:1",
		AccessKeyID:     "unused",
		SecretAccessKey: "unused",
	})
	if err != nil {
		t.Fatalf("NewS3Store failed: %v", err)
	}

	_, logger := logging.New()

	const instanceID = "gateway-partial-boot-instance"

	runCtx, runCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer runCancel()

	runErr := gateway.Run(runCtx, gateway.Deps{
		Consumer:             consumer,
		Publisher:            publisher,
		Manager:              mgr,
		Ownership:            ownershipStore,
		MediaStore:           mediaStore,
		InstanceID:           instanceID,
		ShardLockTTL:         30 * time.Second,
		ShutdownDrainTimeout: 5 * time.Second,
		Logger:               logger,
	})
	if runErr == nil {
		t.Fatal("expected gateway.Run to fail when the send queue no longer exists")
	}

	releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer releaseCancel()
	for shard := 0; shard < shardCount; shard++ {
		ok, err := ownershipStore.Claim(releaseCtx, shard, "another-instance", 5*time.Second)
		if err != nil {
			t.Fatalf("Claim(shard %d) after partial-boot failure failed: %v", shard, err)
		}
		if !ok {
			t.Fatalf("expected shard %d to be released after partial-boot failure, ownership was not released", shard)
		}
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
	for _, rk := range []string{gatewayamqp.ChannelQRRoutingKey, gatewayamqp.ChannelStatusRoutingKey} {
		if err := probeCh.QueueBind(probeQ.Name, rk, gatewayamqp.EventsExchange, false, nil); err != nil {
			t.Fatalf("failed to bind probe queue to %s: %v", rk, err)
		}
	}
	deliveries, err := probeCh.Consume(probeQ.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to consume probe queue: %v", err)
	}

	staleCmd := gatewayamqp.PairCommand{TenantID: "tenant-stale", ChannelID: "channel-stale", UserID: "user-stale"}
	staleBody, err := json.Marshal(staleCmd)
	if err != nil {
		t.Fatalf("failed to marshal stale pair command: %v", err)
	}
	publishCtx, publishCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer publishCancel()
	if err := probeCh.PublishWithContext(publishCtx, gatewayamqp.GatewayPairExchange, "0", false, false, rabbitmq.Publishing{
		ContentType: "application/json",
		Body:        staleBody,
	}); err != nil {
		t.Fatalf("failed to publish stale pair command: %v", err)
	}

	select {
	case d := <-deliveries:
		t.Fatalf("expected no channel.qr/channel.status after a partial-boot failure (the already-started pair consumer must be closed), got routing key %q body %s", d.RoutingKey, d.Body)
	case <-time.After(3 * time.Second):
	}
}

func TestGatewayEndToEnd(t *testing.T) {
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

	fake := newFakeWAClient()
	sentTS := time.Now().Truncate(time.Second)
	fake.sendResp = whatsmeow.SendResponse{ID: "wamid.sent-1", Timestamp: sentTS}

	mgr := session.NewManager(func(channelID string) (session.WAClient, error) {
		return fake, nil
	})

	mediaStore, err := media.NewS3Store(context.Background(), media.S3Config{
		Bucket:          "gateway-e2e-unused",
		Region:          "us-east-1",
		Endpoint:        "http://127.0.0.1:1",
		AccessKeyID:     "unused",
		SecretAccessKey: "unused",
	})
	if err != nil {
		t.Fatalf("NewS3Store failed: %v", err)
	}

	waLogger, logger := logging.New()

	dsn := startPostgresForGateway(t)
	sessionContainer, err := store.Open(context.Background(), dsn, waLogger)
	if err != nil {
		t.Fatalf("store.Open failed: %v", err)
	}
	factory := gateway.NewWAClientFactory(sessionContainer, waLogger)
	if _, err := factory("structural-check-channel"); err != nil {
		t.Fatalf("gateway.NewWAClientFactory-produced factory failed: %v", err)
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
	for _, rk := range []string{
		gatewayamqp.InboundRoutingKey,
		gatewayamqp.StatusRoutingKey,
		gatewayamqp.ChannelQRRoutingKey,
		gatewayamqp.ChannelStatusRoutingKey,
	} {
		if err := probeCh.QueueBind(probeQ.Name, rk, gatewayamqp.EventsExchange, false, nil); err != nil {
			t.Fatalf("failed to bind probe queue to %s: %v", rk, err)
		}
	}
	deliveries, err := probeCh.Consume(probeQ.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to consume probe queue: %v", err)
	}

	const instanceID = "gateway-e2e-instance"
	ctx, cancel := context.WithCancel(context.Background())

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- gateway.Run(ctx, gateway.Deps{
			Consumer:             consumer,
			Publisher:            publisher,
			Manager:              mgr,
			Ownership:            ownershipStore,
			MediaStore:           mediaStore,
			InstanceID:           instanceID,
			ShardLockTTL:         30 * time.Second,
			ShutdownDrainTimeout: 10 * time.Second,
			Logger:               logger,
		})
	}()

	const channelID = "channel-e2e-1"
	const tenantID = "tenant-e2e-1"

	fake.qrItems = []whatsmeow.QRChannelItem{
		{Event: "code", Code: "qr-code-e2e"},
		whatsmeow.QRChannelSuccess,
	}

	pairCmd := gatewayamqp.PairCommand{TenantID: tenantID, ChannelID: channelID, UserID: "user-e2e-1"}
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

	qrDelivery := waitForDelivery(t, deliveries, gatewayamqp.ChannelQRRoutingKey, 10*time.Second)
	var qrEvt gatewayamqp.ChannelQREvent
	if err := json.Unmarshal(qrDelivery.Body, &qrEvt); err != nil {
		t.Fatalf("failed to unmarshal channel.qr event: %v", err)
	}
	if qrEvt.QR != "qr-code-e2e" || qrEvt.ChannelID != channelID || qrEvt.UserID != "user-e2e-1" || qrEvt.TenantID != tenantID {
		t.Fatalf("unexpected channel.qr event: %+v", qrEvt)
	}

	statusDelivery := waitForDelivery(t, deliveries, gatewayamqp.ChannelStatusRoutingKey, 10*time.Second)
	var statusEvt gatewayamqp.ChannelStatusEvent
	if err := json.Unmarshal(statusDelivery.Body, &statusEvt); err != nil {
		t.Fatalf("failed to unmarshal channel.status event: %v", err)
	}
	if statusEvt.Status != "connected" || statusEvt.ChannelID != channelID {
		t.Fatalf("unexpected channel.status event: %+v", statusEvt)
	}

	sendCmd := gatewayamqp.GatewaySendCommand{
		TenantID:  tenantID,
		ChannelID: channelID,
		MessageID: "msg-e2e-1",
		To:        "+15551234567",
		Type:      "text",
		Text:      "hello from e2e",
	}
	sendBody, err := json.Marshal(sendCmd)
	if err != nil {
		t.Fatalf("failed to marshal send command: %v", err)
	}
	if err := probeCh.PublishWithContext(publishCtx, gatewayamqp.GatewaySendExchange, "0", false, false, rabbitmq.Publishing{
		ContentType: "application/json",
		Body:        sendBody,
	}); err != nil {
		t.Fatalf("failed to publish send command: %v", err)
	}

	sentDelivery := waitForDelivery(t, deliveries, gatewayamqp.StatusRoutingKey, 10*time.Second)
	var sentEvt mapper.StatusEvent
	if err := json.Unmarshal(sentDelivery.Body, &sentEvt); err != nil {
		t.Fatalf("failed to unmarshal whatsapp.status.v1 event: %v", err)
	}
	if sentEvt.Status != "sent" || sentEvt.ProviderMessageID != "wamid.sent-1" {
		t.Fatalf("unexpected whatsapp.status.v1 event: %+v", sentEvt)
	}
	if fake.sendCallCount() != 1 {
		t.Fatalf("expected fake WAClient.SendMessage called once, got %d", fake.sendCallCount())
	}
	if fake.lastMsg.GetConversation() != "hello from e2e" {
		t.Fatalf("expected fake to receive the outbound text message, got %+v", fake.lastMsg)
	}

	inboundMsg := &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{Sender: types.NewJID("15559876543", types.DefaultUserServer)},
			ID:            "wamid.inbound-1",
			PushName:      "Jane Doe",
			Timestamp:     time.Now(),
		},
		Message: &waE2E.Message{Conversation: proto.String("hi there")},
	}
	fake.emit(inboundMsg)

	inboundDelivery := waitForDelivery(t, deliveries, gatewayamqp.InboundRoutingKey, 10*time.Second)
	var inboundEvt mapper.InboundEvent
	if err := json.Unmarshal(inboundDelivery.Body, &inboundEvt); err != nil {
		t.Fatalf("failed to unmarshal whatsapp.inbound.v1 event: %v", err)
	}
	if inboundEvt.ProviderMessageID != "wamid.inbound-1" || inboundEvt.Type != "text" || inboundEvt.Text == nil || inboundEvt.Text.Body != "hi there" {
		t.Fatalf("unexpected whatsapp.inbound.v1 event: %+v", inboundEvt)
	}
	if inboundEvt.PhoneNumberID != channelID {
		t.Fatalf("expected phoneNumberId=%s, got %s", channelID, inboundEvt.PhoneNumberID)
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

	if fake.disconnectCount() != 1 {
		t.Fatalf("expected the session to be disconnected exactly once on shutdown, got %d", fake.disconnectCount())
	}

	releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer releaseCancel()
	for shard := 0; shard < shardCount; shard++ {
		ok, err := ownershipStore.Claim(releaseCtx, shard, "another-instance", 5*time.Second)
		if err != nil {
			t.Fatalf("Claim(shard %d) after shutdown failed: %v", shard, err)
		}
		if !ok {
			t.Fatalf("expected shard %d to be released after gateway shutdown", shard)
		}
	}
}
