package test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	rabbitmq "github.com/rabbitmq/amqp091-go"
	"go.mau.fi/whatsmeow/types"

	gatewayamqp "github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/channelsettings"
	"github.com/w3nder/whatsmeow-gateway/internal/gateway"
	"github.com/w3nder/whatsmeow-gateway/internal/logging"
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
	})

	want := channelsettings.Settings{ListenGroups: false, ReceiveCalls: false, CallRejectMessage: "Não atendemos ligações, escreva aqui."}
	waitFor(t, 10*time.Second, "the settings command to reach the registry", func() bool {
		got, found, err := h.registry.Get(context.Background(), channelID)
		return err == nil && found && got.Settings == want
	})
}
