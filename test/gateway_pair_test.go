package test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	rabbitmq "github.com/rabbitmq/amqp091-go"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"

	gatewayamqp "github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/gateway"
	"github.com/w3nder/whatsmeow-gateway/internal/logging"
	"github.com/w3nder/whatsmeow-gateway/internal/media"
	"github.com/w3nder/whatsmeow-gateway/internal/ownership"
	"github.com/w3nder/whatsmeow-gateway/internal/registry"
	"github.com/w3nder/whatsmeow-gateway/internal/session"
)

// setupPairGateway runs a real gateway against a real broker with the channel.qr and
// channel.status events on a probe queue, which is the only place these tests can see
// whether a pair command was ever picked up.
func setupPairGateway(t *testing.T, factory session.ClientFactory) (probeCh *rabbitmq.Channel, deliveries <-chan rabbitmq.Delivery, cancel context.CancelFunc, runErrCh chan error) {
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

	mediaStore, err := media.NewS3Store(context.Background(), media.S3Config{
		Bucket:          "gateway-pair-unused",
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
	registryStore, err := registry.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("registry.Open failed: %v", err)
	}
	t.Cleanup(registryStore.Close)

	probeCh, err = conn.Channel()
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
	for _, rk := range []string{gatewayamqp.ChannelQRRoutingKey, gatewayamqp.ChannelStatusRoutingKey} {
		if err := probeCh.QueueBind(probeQ.Name, rk, gatewayamqp.EventsExchange, false, nil); err != nil {
			t.Fatalf("failed to bind probe queue to %s: %v", rk, err)
		}
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
			Manager:              session.NewManager(factory),
			Ownership:            ownership.NewStore(redisClient, 4),
			Registry:             registryStore,
			MediaStore:           mediaStore,
			InstanceID:           "gateway-pair-instance",
			ShardLockTTL:         30 * time.Second,
			ShutdownDrainTimeout: 10 * time.Second,
			Logger:               logger,
		})
	}()

	return probeCh, deliveries, cancel, runErrCh
}

func publishPairCommand(t *testing.T, probeCh *rabbitmq.Channel, cmd gatewayamqp.PairCommand) {
	t.Helper()

	body, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("failed to marshal pair command for %s: %v", cmd.ChannelID, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := probeCh.PublishWithContext(ctx, gatewayamqp.GatewayPairExchange, "0", false, false, rabbitmq.Publishing{
		ContentType: "application/json",
		Body:        body,
	}); err != nil {
		t.Fatalf("failed to publish pair command for %s: %v", cmd.ChannelID, err)
	}
}

// pairWait is how long these tests give the broker and the gateway to do something they
// would otherwise never do. Every deadline here separates "answered" from "never
// answered" -- a pair command queued behind another one waits for a session that only
// ends at shutdown -- so the slack costs the assertions nothing and keeps a loaded
// machine from reading as a regression.
const pairWait = 30 * time.Second

// feedQR hands the pairing loop its next item, failing the test rather than blocking
// forever when nothing is reading -- which is precisely what a pair command stuck
// behind another one looks like from here.
func feedQR(t *testing.T, fake *fakeWAClient, item whatsmeow.QRChannelItem) {
	t.Helper()

	select {
	case fake.qrFeed <- item:
	case <-time.After(pairWait):
		t.Fatalf("timed out feeding %q to the pairing loop", item.Event)
	}
}

func waitForChannelQR(t *testing.T, deliveries <-chan rabbitmq.Delivery, channelID string, timeout time.Duration) gatewayamqp.ChannelQREvent {
	t.Helper()

	deadline := time.After(timeout)
	for {
		select {
		case d := <-deliveries:
			if d.RoutingKey != gatewayamqp.ChannelQRRoutingKey {
				continue
			}
			var evt gatewayamqp.ChannelQREvent
			if err := json.Unmarshal(d.Body, &evt); err != nil {
				t.Fatalf("failed to unmarshal channel.qr event: %v", err)
			}
			if evt.ChannelID == channelID {
				return evt
			}
		case <-deadline:
			t.Fatalf("timed out waiting for a channel.qr event for %s", channelID)
		}
	}
}

func waitForChannelStatus(t *testing.T, deliveries <-chan rabbitmq.Delivery, channelID, status string, timeout time.Duration) gatewayamqp.ChannelStatusEvent {
	t.Helper()

	deadline := time.After(timeout)
	for {
		select {
		case d := <-deliveries:
			if d.RoutingKey != gatewayamqp.ChannelStatusRoutingKey {
				continue
			}
			var evt gatewayamqp.ChannelStatusEvent
			if err := json.Unmarshal(d.Body, &evt); err != nil {
				t.Fatalf("failed to unmarshal channel.status event: %v", err)
			}
			if evt.ChannelID == channelID && evt.Status == status {
				return evt
			}
		case <-deadline:
			t.Fatalf("timed out waiting for a channel.status %q event for %s", status, channelID)
		}
	}
}

// TestGatewaySecondPairCommandIsAnsweredWhileTheFirstIsInFlight is the reopened dialog
// as the operator actually meets it, over the queue rather than in the manager: the
// first command is still following its pairing session, and the second one -- which the
// retained code exists to answer -- must be picked up and answered anyway.
//
// The gateway is shut down with the session still live, which also pins that a pairing
// no one will finish does not hold shutdown open.
func TestGatewaySecondPairCommandIsAnsweredWhileTheFirstIsInFlight(t *testing.T) {
	fake := newFakeWAClient()
	fake.qrFeed = make(chan whatsmeow.QRChannelItem)

	probeCh, deliveries, cancel, runErrCh := setupPairGateway(t, func(channelID string, jid *types.JID) (session.WAClient, error) {
		return fake, nil
	})

	const (
		channelID = "channel-pair-reopen-1"
		tenantID  = "tenant-pair-reopen-1"
	)

	publishPairCommand(t, probeCh, gatewayamqp.PairCommand{TenantID: tenantID, ChannelID: channelID, UserID: "user-first"})
	feedQR(t, fake, whatsmeow.QRChannelItem{Event: "code", Code: "qr-live", Timeout: time.Minute})

	first := waitForChannelQR(t, deliveries, channelID, pairWait)
	if first.QR != "qr-live" || first.UserID != "user-first" {
		t.Fatalf("unexpected first channel.qr event: %+v", first)
	}

	// The dialog is closed and reopened. Nothing is fed to the pairing loop in the
	// meantime, so the only code that can come back is the retained one -- and it can
	// only come back if this command was consumed while the first session is running.
	publishPairCommand(t, probeCh, gatewayamqp.PairCommand{TenantID: tenantID, ChannelID: channelID, UserID: "user-second"})

	second := waitForChannelQR(t, deliveries, channelID, pairWait)
	if second.QR != "qr-live" {
		t.Fatalf("expected the reopened dialog to get the code that is on the wire now, got %+v", second)
	}
	if second.UserID != "user-second" {
		t.Fatalf("expected the retained code to go back to the operator who asked for it, got %+v", second)
	}

	if calls := fake.qrChannelCallCount(); calls != 1 {
		t.Fatalf("expected the second command to join the live session, got %d QRChannel calls", calls)
	}

	shutdownStatusRoundtripGateway(t, cancel, runErrCh)
}

// TestGatewayPairForOneChannelDoesNotWaitForAnother: one operator's pairing used to
// stall every other operator's on the same instance, which is worse than the reported
// symptom and invisible to the channel it happens to.
func TestGatewayPairForOneChannelDoesNotWaitForAnother(t *testing.T) {
	const (
		slowChannel = "channel-pair-slow-1"
		fastChannel = "channel-pair-fast-1"
	)

	slow := newFakeWAClient()
	slow.qrFeed = make(chan whatsmeow.QRChannelItem)
	fast := newFakeWAClient()
	fast.qrItems = []whatsmeow.QRChannelItem{{Event: "code", Code: "qr-fast", Timeout: time.Minute}}

	probeCh, deliveries, cancel, runErrCh := setupPairGateway(t, func(channelID string, jid *types.JID) (session.WAClient, error) {
		if channelID == fastChannel {
			return fast, nil
		}
		return slow, nil
	})

	publishPairCommand(t, probeCh, gatewayamqp.PairCommand{TenantID: "tenant-pair-slow", ChannelID: slowChannel, UserID: "user-slow"})
	feedQR(t, slow, whatsmeow.QRChannelItem{Event: "code", Code: "qr-slow", Timeout: time.Minute})
	waitForChannelQR(t, deliveries, slowChannel, pairWait)

	// The first channel's session is now parked on its feed, exactly as a real one is
	// parked on the operator's phone.
	publishPairCommand(t, probeCh, gatewayamqp.PairCommand{TenantID: "tenant-pair-fast", ChannelID: fastChannel, UserID: "user-fast"})

	got := waitForChannelQR(t, deliveries, fastChannel, pairWait)
	if got.QR != "qr-fast" {
		t.Fatalf("expected %s to be paired without waiting for %s, got %+v", fastChannel, slowChannel, got)
	}

	shutdownStatusRoundtripGateway(t, cancel, runErrCh)
}

// TestGatewayPairFailureIsAckedUnlessThePairingNeverStarted fixes what the queue now
// means for a failed pairing. Once the command is accepted, the operator not scanning
// in time is reported to that operator as channel.status error and nothing else: it is
// their outcome, not a command to replay or a fault to dead-letter. A pairing this
// instance could not start at all never reaches that point, so it still dead-letters --
// which is what keeps a broken instance distinguishable from an idle operator.
func TestGatewayPairFailureIsAckedUnlessThePairingNeverStarted(t *testing.T) {
	const (
		timeoutChannel     = "channel-pair-timeout-1"
		unstartableChannel = "channel-pair-unstartable-1"
	)

	timesOut := newFakeWAClient()
	timesOut.qrItems = []whatsmeow.QRChannelItem{
		{Event: "code", Code: "qr-never-scanned", Timeout: time.Minute},
		whatsmeow.QRChannelTimeout,
	}

	probeCh, deliveries, cancel, runErrCh := setupPairGateway(t, func(channelID string, jid *types.JID) (session.WAClient, error) {
		if channelID == unstartableChannel {
			return nil, fmt.Errorf("no stored device for channel %s", channelID)
		}
		return timesOut, nil
	})

	dlq, err := probeCh.Consume(gatewayamqp.GatewayPairDLQ, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to consume %s: %v", gatewayamqp.GatewayPairDLQ, err)
	}

	publishPairCommand(t, probeCh, gatewayamqp.PairCommand{TenantID: "tenant-pair-timeout", ChannelID: timeoutChannel, UserID: "user-timeout"})

	evt := waitForChannelStatus(t, deliveries, timeoutChannel, "error", pairWait)
	if evt.Reason == "" {
		t.Fatalf("expected the timed-out pairing to carry a reason the operator can read, got %+v", evt)
	}

	select {
	case d := <-dlq:
		t.Fatalf("expected a pairing the operator never finished to be acked, got it dead-lettered: %s", d.Body)
	case <-time.After(3 * time.Second):
	}

	publishPairCommand(t, probeCh, gatewayamqp.PairCommand{TenantID: "tenant-pair-unstartable", ChannelID: unstartableChannel, UserID: "user-unstartable"})

	select {
	case d := <-dlq:
		var got gatewayamqp.PairCommand
		if err := json.Unmarshal(d.Body, &got); err != nil {
			t.Fatalf("failed to unmarshal dead-lettered pair command: %v", err)
		}
		if got.ChannelID != unstartableChannel {
			t.Fatalf("expected %s on the dlq, got %+v", unstartableChannel, got)
		}
	case <-time.After(pairWait):
		t.Fatalf("expected a pairing that could not be started at all to reach %s", gatewayamqp.GatewayPairDLQ)
	}

	shutdownStatusRoundtripGateway(t, cancel, runErrCh)
}
