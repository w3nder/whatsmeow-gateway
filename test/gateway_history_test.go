package test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	rabbitmq "github.com/rabbitmq/amqp091-go"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	gatewayamqp "github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/gateway"
	"github.com/w3nder/whatsmeow-gateway/internal/logging"
	"github.com/w3nder/whatsmeow-gateway/internal/ownership"
	"github.com/w3nder/whatsmeow-gateway/internal/registry"
	"github.com/w3nder/whatsmeow-gateway/internal/session"
)

type historyGateway struct {
	registry   *registry.Store
	probeCh    *rabbitmq.Channel
	deliveries <-chan rabbitmq.Delivery
}

func startHistoryGateway(t *testing.T, fake *fakeWAClient, seeds ...func(*registry.Store) error) historyGateway {
	t.Helper()
	conn := startRabbitMQ(t)
	redisClient := startRedis(t)
	dsn := startPostgresForGateway(t)

	registryStore, err := registry.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("registry.Open failed: %v", err)
	}
	t.Cleanup(registryStore.Close)
	for _, seed := range seeds {
		if err := seed(registryStore); err != nil {
			t.Fatalf("seed the registry: %v", err)
		}
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
	mediaStore := newUnusedMediaStore(t, "gateway-history-unused")
	_, logger := logging.New()

	probeCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open probe channel: %v", err)
	}
	t.Cleanup(func() {
		if err := probeCh.Close(); err != nil && !errors.Is(err, rabbitmq.ErrClosed) {
			t.Errorf("failed to close probe channel: %v", err)
		}
	})
	probeQ, err := probeCh.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatalf("failed to declare probe queue: %v", err)
	}
	for _, key := range []string{gatewayamqp.HistoryRoutingKey, gatewayamqp.ChannelStatusRoutingKey} {
		if err := probeCh.QueueBind(probeQ.Name, key, gatewayamqp.EventsExchange, false, nil); err != nil {
			t.Fatalf("failed to bind probe queue to %s: %v", key, err)
		}
	}
	deliveries, err := probeCh.Consume(probeQ.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to consume probe queue: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- gateway.Run(ctx, gateway.Deps{
			Rpc:                  gatewayamqp.NewRpcServer(conn, 4, logger),
			Consumer:             consumer,
			Publisher:            publisher,
			Manager:              session.NewManager(func(string, *types.JID) (session.WAClient, error) { return fake, nil }),
			Ownership:            ownership.NewStore(redisClient, 4),
			Registry:             registryStore,
			MediaStore:           mediaStore,
			InstanceID:           "gateway-history-instance",
			ShardLockTTL:         30 * time.Second,
			ShutdownDrainTimeout: 10 * time.Second,
			Logger:               logger,
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case runErr := <-runErrCh:
			if runErr != nil {
				t.Errorf("gateway.Run returned an error on shutdown: %v", runErr)
			}
		case <-time.After(15 * time.Second):
			t.Error("gateway.Run did not return after ctx cancellation")
		}
	})

	return historyGateway{registry: registryStore, probeCh: probeCh, deliveries: deliveries}
}

func historyNotificationEvent() *events.Message {
	own := types.NewJID("15550000000", types.DefaultUserServer)
	return &events.Message{
		Info: types.MessageInfo{MessageSource: types.MessageSource{Chat: own, Sender: own, IsFromMe: true}, ID: "3EB0NOTIF"},
		Message: &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{
			Type:                    waE2E.ProtocolMessage_HISTORY_SYNC_NOTIFICATION.Enum(),
			HistorySyncNotification: &waE2E.HistorySyncNotification{DirectPath: proto.String("/v/t62.history/fake")},
		}},
	}
}

func historyFinalChunk(now time.Time) *waHistorySync.HistorySync {
	message := func(chat, id string) *waHistorySync.HistorySyncMsg {
		return &waHistorySync.HistorySyncMsg{Message: &waWeb.WebMessageInfo{
			Key:              &waCommon.MessageKey{RemoteJID: proto.String(chat), FromMe: proto.Bool(false), ID: proto.String(id)},
			MessageTimestamp: proto.Uint64(uint64(now.Add(-time.Hour).Unix())),
			PushName:         proto.String("Maria"),
			Message:          &waE2E.Message{Conversation: proto.String("Olá")},
		}}
	}
	return &waHistorySync.HistorySync{
		SyncType:   waHistorySync.HistorySync_RECENT.Enum(),
		ChunkOrder: proto.Uint32(1),
		Progress:   proto.Uint32(100),
		Conversations: []*waHistorySync.Conversation{
			{ID: proto.String("5511999998888@s.whatsapp.net"), Messages: []*waHistorySync.HistorySyncMsg{message("5511999998888@s.whatsapp.net", "3EB0HIST1")}},
			{ID: proto.String("120363000000000000@g.us"), Messages: []*waHistorySync.HistorySyncMsg{message("120363000000000000@g.us", "3EB0HISTG")}},
		},
	}
}

func waitForHistoryMessage(t *testing.T, deliveries <-chan rabbitmq.Delivery, kind string, timeout time.Duration) []byte {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case d := <-deliveries:
			if d.RoutingKey != gatewayamqp.HistoryRoutingKey {
				continue
			}
			var head struct {
				Kind string `json:"kind"`
			}
			if err := json.Unmarshal(d.Body, &head); err != nil {
				t.Fatalf("history message is not json: %v", err)
			}
			if head.Kind == kind {
				return d.Body
			}
		case <-deadline:
			t.Fatalf("timed out waiting for a %s history message", kind)
		}
	}
}

func assertNoHistoryMessage(t *testing.T, deliveries <-chan rabbitmq.Delivery, within time.Duration) {
	t.Helper()
	deadline := time.After(within)
	for {
		select {
		case d := <-deliveries:
			if d.RoutingKey == gatewayamqp.HistoryRoutingKey {
				t.Fatalf("no history message may be published without an active import, got %s", d.Body)
			}
		case <-deadline:
			return
		}
	}
}

func TestGatewayImportsTheHistoryOfAFreshPairingAndEndsTheImport(t *testing.T) {
	const (
		channelID = "channel-history-1"
		tenantID  = "tenant-history-1"
		importID  = "import-history-1"
	)
	fake := newFakeWAClient()
	fake.qrItems = []whatsmeow.QRChannelItem{{Event: "code", Code: "qr-history-1"}, whatsmeow.QRChannelSuccess}
	fake.historyChunk = historyFinalChunk(time.Now())
	h := startHistoryGateway(t, fake)
	ctx := context.Background()

	publishPairCommand(t, h.probeCh, gatewayamqp.PairCommand{TenantID: tenantID, ChannelID: channelID, UserID: "user-history-1", ImportHistory: true, ImportID: importID})
	waitForChannelStatus(t, h.deliveries, channelID, "connected", pairWait)
	waitFor(t, 10*time.Second, "the import to be recorded before any chunk", func() bool {
		active, found, err := h.registry.ActiveHistoryImport(ctx, channelID)
		return err == nil && found && active.ImportID == importID && active.TenantID == tenantID
	})

	fake.emit(historyNotificationEvent())

	var batch gatewayamqp.HistoryBatch
	if err := json.Unmarshal(waitForHistoryMessage(t, h.deliveries, gatewayamqp.HistoryBatchKind, 20*time.Second), &batch); err != nil {
		t.Fatalf("unmarshal batch: %v", err)
	}
	if batch.ImportID != importID || batch.TenantID != tenantID || batch.ChannelID != channelID || batch.SourceProgress != 100 || batch.BatchIndex != 1 || batch.BatchesInChunk != 1 {
		t.Fatalf("batch header wrong: %+v", batch)
	}
	if len(batch.Chats) != 1 || batch.Chats[0].Phone != "5511999998888" || batch.Chats[0].ProfileName == nil || *batch.Chats[0].ProfileName != "Maria" || len(batch.Chats[0].Messages) != 1 {
		t.Fatalf("only the private chat is imported, got %+v", batch.Chats)
	}
	if message := string(batch.Chats[0].Messages[0]); !strings.Contains(message, `"providerMessageId":"3EB0HIST1"`) || strings.Contains(message, `"from"`) {
		t.Fatalf("the message is the live event without the chat fields, got %s", message)
	}

	var done gatewayamqp.HistoryDone
	if err := json.Unmarshal(waitForHistoryMessage(t, h.deliveries, gatewayamqp.HistoryDoneKind, 20*time.Second), &done); err != nil {
		t.Fatalf("unmarshal done: %v", err)
	}
	if done.ImportID != importID || done.TotalBatches != 1 {
		t.Fatalf("done must close the import with its batch total, got %+v", done)
	}
	waitFor(t, 10*time.Second, "the import to end in the registry", func() bool {
		_, found, err := h.registry.ActiveHistoryImport(ctx, channelID)
		return err == nil && !found
	})
	waitFor(t, 10*time.Second, "the chunk to be released after the confirm", func() bool {
		return fake.releaseCount() == 1
	})
}

func TestGatewayLeavesTheHistoryOfAChannelWithoutAnImportToWhatsmeow(t *testing.T) {
	const channelID = "channel-history-2"
	fake := newFakeWAClient()
	fake.qrItems = []whatsmeow.QRChannelItem{{Event: "code", Code: "qr-history-2"}, whatsmeow.QRChannelSuccess}
	fake.historyChunk = historyFinalChunk(time.Now())
	h := startHistoryGateway(t, fake)

	publishPairCommand(t, h.probeCh, gatewayamqp.PairCommand{TenantID: "tenant-history-2", ChannelID: channelID, UserID: "user-history-2"})
	waitForChannelStatus(t, h.deliveries, channelID, "connected", pairWait)

	fake.emit(historyNotificationEvent())

	assertNoHistoryMessage(t, h.deliveries, 2*time.Second)
	if fake.TakesOverHistory() || fake.historyDownloadCount() != 0 || fake.releaseCount() != 0 {
		t.Fatalf("a channel without an import keeps today's automatic download: taken over %v, downloads %d, releases %d",
			fake.TakesOverHistory(), fake.historyDownloadCount(), fake.releaseCount())
	}
}

func TestGatewayResumesAChannelInTheMiddleOfAnImport(t *testing.T) {
	const (
		channelID = "channel-history-4"
		tenantID  = "tenant-history-4"
		importID  = "import-history-4"
	)
	fake := newFakeWAClient()
	fake.historyChunk = historyFinalChunk(time.Now())
	h := startHistoryGateway(t, fake, func(store *registry.Store) error {
		if err := store.Save(context.Background(), channelID, "15550004444@s.whatsapp.net", tenantID); err != nil {
			return err
		}
		return store.BeginHistoryImport(context.Background(), channelID, tenantID, importID)
	})

	waitFor(t, 10*time.Second, "the boot resume to connect the stored session", func() bool {
		return fake.connectCallCount() > 0
	})
	if !fake.tookOverHistoryBeforeConnecting() {
		t.Fatal("a channel resumed with an active import must download its history chunks itself")
	}

	fake.emit(historyNotificationEvent())

	var batch gatewayamqp.HistoryBatch
	if err := json.Unmarshal(waitForHistoryMessage(t, h.deliveries, gatewayamqp.HistoryBatchKind, 20*time.Second), &batch); err != nil {
		t.Fatalf("unmarshal batch: %v", err)
	}
	if batch.ImportID != importID || batch.TenantID != tenantID {
		t.Fatalf("the resumed import keeps its id, got %+v", batch)
	}
}

func TestGatewayPublishesNothingForAnImportReplacedByASecondPairRequest(t *testing.T) {
	const channelID = "channel-history-5"
	fake := newFakeWAClient()
	fake.qrFeed = make(chan whatsmeow.QRChannelItem)
	fake.historyChunk = historyFinalChunk(time.Now())
	h := startHistoryGateway(t, fake)
	ctx := context.Background()

	publishPairCommand(t, h.probeCh, gatewayamqp.PairCommand{TenantID: "tenant-history-5", ChannelID: channelID, UserID: "user-first", ImportHistory: true, ImportID: "import-first"})
	feedQR(t, fake, whatsmeow.QRChannelItem{Event: "code", Code: "qr-replaced", Timeout: time.Minute})
	publishPairCommand(t, h.probeCh, gatewayamqp.PairCommand{TenantID: "tenant-history-5", ChannelID: channelID, UserID: "user-second", ImportHistory: true, ImportID: "import-second"})
	waitFor(t, 10*time.Second, "the second request to replace the import", func() bool {
		active, found, err := h.registry.ActiveHistoryImport(ctx, channelID)
		return err == nil && found && active.ImportID == "import-second"
	})
	fake.markPaired()
	feedQR(t, fake, whatsmeow.QRChannelSuccess)
	close(fake.qrFeed)
	waitForChannelStatus(t, h.deliveries, channelID, "connected", pairWait)

	fake.emit(historyNotificationEvent())

	for _, kind := range []string{gatewayamqp.HistoryBatchKind, gatewayamqp.HistoryDoneKind} {
		var head struct {
			ImportID string `json:"importId"`
		}
		if err := json.Unmarshal(waitForHistoryMessage(t, h.deliveries, kind, 20*time.Second), &head); err != nil {
			t.Fatalf("unmarshal %s: %v", kind, err)
		}
		if head.ImportID != "import-second" {
			t.Fatalf("the replaced import must never be published, got a %s for %s", kind, head.ImportID)
		}
	}
}

func TestGatewayEndsTheImportOfAnAlreadyPairedChannelWithNoBatches(t *testing.T) {
	const (
		channelID = "channel-history-3"
		importID  = "import-history-3"
	)
	fake := newFakeWAClient()
	fake.markPaired()
	h := startHistoryGateway(t, fake)

	publishPairCommand(t, h.probeCh, gatewayamqp.PairCommand{TenantID: "tenant-history-3", ChannelID: channelID, UserID: "user-history-3", ImportHistory: true, ImportID: importID})

	var done gatewayamqp.HistoryDone
	if err := json.Unmarshal(waitForHistoryMessage(t, h.deliveries, gatewayamqp.HistoryDoneKind, pairWait), &done); err != nil {
		t.Fatalf("unmarshal done: %v", err)
	}
	if done.ImportID != importID || done.TotalBatches != 0 {
		t.Fatalf("an already paired channel gets no history, so its import ends empty, got %+v", done)
	}
	waitFor(t, 10*time.Second, "the empty import to end in the registry", func() bool {
		_, found, err := h.registry.ActiveHistoryImport(context.Background(), channelID)
		return err == nil && !found
	})
}

func TestGatewayRestartsAPairingWithoutHistoryWhenAnImportIsRequested(t *testing.T) {
	const (
		channelID = "channel-history-6"
		importID  = "import-history-6"
	)
	fake := newFakeWAClient()
	fake.qrFeed = make(chan whatsmeow.QRChannelItem)
	fake.historyChunk = historyFinalChunk(time.Now())
	h := startHistoryGateway(t, fake)

	publishPairCommand(t, h.probeCh, gatewayamqp.PairCommand{TenantID: "tenant-history-6", ChannelID: channelID, UserID: "user-plain"})
	feedQR(t, fake, whatsmeow.QRChannelItem{Event: "code", Code: "qr-plain", Timeout: time.Minute})
	publishPairCommand(t, h.probeCh, gatewayamqp.PairCommand{TenantID: "tenant-history-6", ChannelID: channelID, UserID: "user-importing", ImportHistory: true, ImportID: importID})
	waitFor(t, 10*time.Second, "the pairing without history to be dropped and restarted with the history", func() bool {
		return fake.disconnectCount() > 0 && fake.TakesOverHistory()
	})
	feedQR(t, fake, whatsmeow.QRChannelItem{Event: "code", Code: "qr-importing", Timeout: time.Minute})
	fake.markPaired()
	feedQR(t, fake, whatsmeow.QRChannelSuccess)
	close(fake.qrFeed)
	waitForChannelStatus(t, h.deliveries, channelID, "connected", pairWait)

	fake.emit(historyNotificationEvent())

	var batch gatewayamqp.HistoryBatch
	if err := json.Unmarshal(waitForHistoryMessage(t, h.deliveries, gatewayamqp.HistoryBatchKind, 20*time.Second), &batch); err != nil {
		t.Fatalf("unmarshal batch: %v", err)
	}
	if batch.ImportID != importID || len(batch.Chats) != 1 {
		t.Fatalf("the restarted pairing must deliver the requested import, got %+v", batch)
	}
}
