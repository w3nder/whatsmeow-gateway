package test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	rabbitmq "github.com/rabbitmq/amqp091-go"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	gatewayamqp "github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/call"
	"github.com/w3nder/whatsmeow-gateway/internal/channelsettings"
	"github.com/w3nder/whatsmeow-gateway/internal/gateway"
	"github.com/w3nder/whatsmeow-gateway/internal/logging"
	"github.com/w3nder/whatsmeow-gateway/internal/mapper"
	"github.com/w3nder/whatsmeow-gateway/internal/ownership"
	"github.com/w3nder/whatsmeow-gateway/internal/registry"
	"github.com/w3nder/whatsmeow-gateway/internal/session"
)

type settingsGateway struct {
	registry   *registry.Store
	publishCh  *rabbitmq.Channel
	deliveries <-chan rabbitmq.Delivery
}

func startSettingsGateway(t *testing.T, channelID, tenantID string, fake *fakeWAClient, stored channelsettings.Settings) settingsGateway {
	t.Helper()
	conn := startRabbitMQ(t)
	redisClient := startRedis(t)
	dsn := startPostgresForGateway(t)
	ctx := context.Background()

	registryStore, err := registry.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("registry.Open failed: %v", err)
	}
	t.Cleanup(registryStore.Close)

	storedJID := types.NewJID("15550003333", types.DefaultUserServer)
	if err := registryStore.Save(ctx, channelID, storedJID.String(), tenantID); err != nil {
		t.Fatalf("registry.Save failed: %v", err)
	}
	if err := registryStore.SaveSettings(ctx, channelID, tenantID, stored); err != nil {
		t.Fatalf("registry.SaveSettings failed: %v", err)
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

	mgr := session.NewManager(func(string, *types.JID) (session.WAClient, error) {
		return fake, nil
	})
	mediaStore := newUnusedMediaStore(t, "gateway-settings-unused")
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
	for _, key := range []string{gatewayamqp.InboundRoutingKey, gatewayamqp.GroupInboundRoutingKey} {
		if err := probeCh.QueueBind(probeQ.Name, key, gatewayamqp.EventsExchange, false, nil); err != nil {
			t.Fatalf("failed to bind probe queue to %s: %v", key, err)
		}
	}
	deliveries, err := probeCh.Consume(probeQ.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to consume probe queue: %v", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- gateway.Run(ctx, gateway.Deps{
			Rpc:                  gatewayamqp.NewRpcServer(conn, 4, logger),
			Consumer:             consumer,
			Publisher:            publisher,
			Manager:              mgr,
			Ownership:            ownership.NewStore(redisClient, 4),
			Registry:             registryStore,
			MediaStore:           mediaStore,
			InstanceID:           "gateway-settings-instance",
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

	waitFor(t, 10*time.Second, "the boot resume to connect the stored session", func() bool {
		return fake.connectCallCount() > 0
	})

	return settingsGateway{registry: registryStore, publishCh: probeCh, deliveries: deliveries}
}

func (h settingsGateway) publishSettings(t *testing.T, cmd gatewayamqp.SettingsCommand) {
	t.Helper()
	body, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("failed to marshal settings command: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.publishCh.PublishWithContext(ctx, gatewayamqp.GatewaySettingsExchange, "0", false, false, rabbitmq.Publishing{
		ContentType: "application/json",
		Body:        body,
	}); err != nil {
		t.Fatalf("failed to publish settings command: %v", err)
	}
}

func settingsBool(v bool) *bool { return &v }

func settingsString(v string) *string { return &v }

func TestGatewayAppliesASettingsCommandAndPersistsItInTheRegistry(t *testing.T) {
	const channelID = "channel-settings-command-1"
	const tenantID = "tenant-settings-command-1"
	h := startSettingsGateway(t, channelID, tenantID, newFakeWAClient(), channelsettings.Defaults())

	h.publishSettings(t, gatewayamqp.SettingsCommand{
		TenantID:          tenantID,
		ChannelID:         channelID,
		ListenGroups:      settingsBool(false),
		ReceiveCalls:      settingsBool(false),
		CallRejectMessage: settingsString("Não atendemos ligações, escreva aqui."),
		SettingsVersion:   3,
	})

	want := channelsettings.Settings{ListenGroups: false, ReceiveCalls: false, CallRejectMessage: "Não atendemos ligações, escreva aqui.", Version: 3}
	waitFor(t, 10*time.Second, "the settings command to reach the registry", func() bool {
		got, found, err := h.registry.Get(context.Background(), channelID)
		return err == nil && found && got.Settings == want
	})
}

func TestGatewayIgnoresALowerSettingsVersionAndAppliesEqualAndHigherOnes(t *testing.T) {
	const channelID = "channel-settings-version-1"
	const tenantID = "tenant-settings-version-1"
	h := startSettingsGateway(t, channelID, tenantID, newFakeWAClient(), channelsettings.Settings{ListenGroups: true, ReceiveCalls: true, Version: 5})

	storedSettings := func() channelsettings.Settings {
		got, _, err := h.registry.Get(context.Background(), channelID)
		if err != nil {
			t.Fatalf("registry Get failed: %v", err)
		}
		return got.Settings
	}

	h.publishSettings(t, gatewayamqp.SettingsCommand{TenantID: tenantID, ChannelID: channelID, ListenGroups: settingsBool(false), SettingsVersion: 4})
	time.Sleep(2 * time.Second)
	if got := storedSettings(); got.Version != 5 || !got.ListenGroups {
		t.Fatalf("a lower version must be ignored, stored %+v", got)
	}

	h.publishSettings(t, gatewayamqp.SettingsCommand{TenantID: tenantID, ChannelID: channelID, ListenGroups: settingsBool(false), SettingsVersion: 5})
	waitFor(t, 10*time.Second, "the equal version to be applied", func() bool {
		got := storedSettings()
		return got.Version == 5 && !got.ListenGroups
	})

	h.publishSettings(t, gatewayamqp.SettingsCommand{TenantID: tenantID, ChannelID: channelID, ReceiveCalls: settingsBool(false), SettingsVersion: 8})
	waitFor(t, 10*time.Second, "the higher version to be applied", func() bool {
		got := storedSettings()
		return got.Version == 8 && got.ListenGroups && !got.ReceiveCalls
	})
}

func emitTextMessage(fake *fakeWAClient, chat, sender types.JID, id, body string) {
	fake.emit(&events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{Chat: chat, Sender: sender},
			ID:            id,
			Timestamp:     time.Now(),
		},
		Message: &waE2E.Message{Conversation: proto.String(body)},
	})
}

func TestGatewayDropsGroupEventsForAChannelThatStoredListenGroupsOff(t *testing.T) {
	const channelID = "channel-mute-groups-1"
	fake := newFakeWAClient()
	h := startSettingsGateway(t, channelID, "tenant-mute-groups-1", fake, channelsettings.Settings{ListenGroups: false, ReceiveCalls: true})

	group := types.NewJID("120363000000000001", types.GroupServer)
	member := types.NewJID("5511777776666", types.DefaultUserServer)
	emitTextMessage(fake, group, member, "GROUPMSG1", "oi grupo")
	emitTextMessage(fake, member, member, "PRIVATEMSG1", "oi")

	deadline := time.After(10 * time.Second)
	for {
		select {
		case d := <-h.deliveries:
			if d.RoutingKey == gatewayamqp.GroupInboundRoutingKey {
				t.Fatal("a group message was published although the channel does not listen to groups")
			}
			if d.RoutingKey != gatewayamqp.InboundRoutingKey {
				continue
			}
			var evt mapper.InboundEvent
			if err := json.Unmarshal(d.Body, &evt); err != nil {
				t.Fatalf("failed to unmarshal inbound event: %v", err)
			}
			if evt.ProviderMessageID != "PRIVATEMSG1" {
				t.Fatalf("first published message = %q, want the private one", evt.ProviderMessageID)
			}
			return
		case <-deadline:
			t.Fatal("timed out waiting for the private message")
		}
	}
}

func TestGatewayKeepsPublishingGroupEventsWhenTheChannelListensToGroups(t *testing.T) {
	const channelID = "channel-listen-groups-1"
	fake := newFakeWAClient()
	h := startSettingsGateway(t, channelID, "tenant-listen-groups-1", fake, channelsettings.Defaults())

	group := types.NewJID("120363000000000002", types.GroupServer)
	member := types.NewJID("5511777776666", types.DefaultUserServer)
	emitTextMessage(fake, group, member, "GROUPMSG2", "oi grupo")

	d := waitForDelivery(t, h.deliveries, gatewayamqp.GroupInboundRoutingKey, 10*time.Second)
	var evt mapper.InboundEvent
	if err := json.Unmarshal(d.Body, &evt); err != nil {
		t.Fatalf("failed to unmarshal group inbound event: %v", err)
	}
	if evt.ProviderMessageID != "GROUPMSG2" || evt.Group == nil || evt.Group.JID != group.String() {
		t.Fatalf("unexpected group event: %+v", evt)
	}
}

func TestGatewayRejectsTheCallAndAnswersWithTheStoredMessageWhenTheChannelDoesNotReceiveCalls(t *testing.T) {
	const channelID = "channel-reject-calls-1"
	const text = "Não atendemos ligações, escreva aqui."

	caller := &fakeCaller{}
	fake := newFakeWAClient()
	fake.caller = caller
	fake.sendResp = whatsmeow.SendResponse{ID: "3EB0AUTOREPLY", Timestamp: time.Unix(1754300000, 0)}
	h := startSettingsGateway(t, channelID, "tenant-reject-calls-1", fake, channelsettings.Settings{
		ListenGroups: true, ReceiveCalls: false, CallRejectMessage: text,
	})
	waitFor(t, 10*time.Second, "the calling client to be attached", func() bool {
		return caller.incomingHandler() != nil
	})

	live := &fakeLiveCall{callID: "CALLREJECT1", peer: "5511888887777@s.whatsapp.net"}
	caller.fireIncoming(live)

	callDelivery := waitForDelivery(t, h.deliveries, gatewayamqp.InboundRoutingKey, 10*time.Second)
	var callEvt call.InboundCallEvent
	if err := json.Unmarshal(callDelivery.Body, &callEvt); err != nil {
		t.Fatalf("failed to unmarshal call event: %v", err)
	}
	if callEvt.Type != "call" || callEvt.RichContent == nil || callEvt.RichContent.State != "auto_rejected" {
		t.Fatalf("first event = %+v, want a call in state auto_rejected", callEvt)
	}
	if callEvt.ProviderMessageID != "CALLREJECT1" || callEvt.From != "5511888887777" {
		t.Fatalf("call event identity = %+v", callEvt)
	}

	replyDelivery := waitForDelivery(t, h.deliveries, gatewayamqp.InboundRoutingKey, 10*time.Second)
	var replyEvt mapper.InboundEvent
	if err := json.Unmarshal(replyDelivery.Body, &replyEvt); err != nil {
		t.Fatalf("failed to unmarshal reply event: %v", err)
	}
	if !replyEvt.FromMe || replyEvt.Origin != "call_auto_reply" || replyEvt.ProviderMessageID != "3EB0AUTOREPLY" {
		t.Fatalf("reply event = %+v, want a fromMe message with origin call_auto_reply", replyEvt)
	}
	if replyEvt.Text == nil || replyEvt.Text.Body != text {
		t.Fatalf("reply text = %+v, want %q", replyEvt.Text, text)
	}

	if got := live.recordedActions(); !reflect.DeepEqual(got, []string{"reject"}) {
		t.Fatalf("live call actions = %v, want only reject", got)
	}
	if fake.sendCallCount() != 1 {
		t.Fatalf("whatsapp sends = %d, want exactly 1", fake.sendCallCount())
	}
}

func TestGatewayRejectsWithoutAMessageWhenNoneIsStored(t *testing.T) {
	const channelID = "channel-reject-calls-2"

	caller := &fakeCaller{}
	fake := newFakeWAClient()
	fake.caller = caller
	h := startSettingsGateway(t, channelID, "tenant-reject-calls-2", fake, channelsettings.Settings{ListenGroups: true, ReceiveCalls: false})
	waitFor(t, 10*time.Second, "the calling client to be attached", func() bool {
		return caller.incomingHandler() != nil
	})

	live := &fakeLiveCall{callID: "CALLREJECT2", peer: "5511888887777@s.whatsapp.net"}
	caller.fireIncoming(live)

	callDelivery := waitForDelivery(t, h.deliveries, gatewayamqp.InboundRoutingKey, 10*time.Second)
	var callEvt call.InboundCallEvent
	if err := json.Unmarshal(callDelivery.Body, &callEvt); err != nil {
		t.Fatalf("failed to unmarshal call event: %v", err)
	}
	if callEvt.RichContent == nil || callEvt.RichContent.State != "auto_rejected" {
		t.Fatalf("event = %+v, want state auto_rejected", callEvt)
	}
	if fake.sendCallCount() != 0 {
		t.Fatalf("no message is stored, whatsapp sends = %d, want 0", fake.sendCallCount())
	}
}
