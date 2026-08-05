package test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	rabbitmq "github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go/modules/minio"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"

	gatewayamqp "github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/call"
	"github.com/w3nder/whatsmeow-gateway/internal/dedupe"
	"github.com/w3nder/whatsmeow-gateway/internal/gateway"
	"github.com/w3nder/whatsmeow-gateway/internal/logging"
	"github.com/w3nder/whatsmeow-gateway/internal/media"
	"github.com/w3nder/whatsmeow-gateway/internal/ownership"
	"github.com/w3nder/whatsmeow-gateway/internal/registry"
	"github.com/w3nder/whatsmeow-gateway/internal/session"
	"github.com/w3nder/whatsmeow-gateway/internal/store"
)

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// waitForCallEvent drains call events until one of the wanted type arrives.
// Other types on the same routing key are skipped rather than failed on.
func waitForCallEvent(t *testing.T, deliveries <-chan rabbitmq.Delivery, wantType string, timeout time.Duration) call.Event {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case d := <-deliveries:
			if d.RoutingKey != gatewayamqp.CallRoutingKey {
				continue
			}
			var evt call.Event
			if err := json.Unmarshal(d.Body, &evt); err != nil {
				t.Fatalf("failed to unmarshal call event: %v", err)
			}
			if evt.Type == wantType {
				return evt
			}
		case <-deadline:
			t.Fatalf("timed out waiting for a %q call event", wantType)
		}
	}
}

// waitForIncomingCallAndInboundEvent drains the probe channel until it has
// seen both the "incoming" call.Event and the call.InboundCallEvent an
// arriving call publishes, regardless of which order they land in. Anything
// else on the channel (a channel.status delivery from pairing, an unrelated
// call event type) is ignored rather than consumed and lost, since a single
// pass has to serve both waits without racing a second reader against this
// one for the same messages.
func waitForIncomingCallAndInboundEvent(t *testing.T, deliveries <-chan rabbitmq.Delivery, timeout time.Duration) (call.Event, call.InboundCallEvent) {
	t.Helper()
	var incoming call.Event
	var inbound call.InboundCallEvent
	gotIncoming, gotInbound := false, false
	deadline := time.After(timeout)
	for !gotIncoming || !gotInbound {
		select {
		case d := <-deliveries:
			switch d.RoutingKey {
			case gatewayamqp.CallRoutingKey:
				var evt call.Event
				if err := json.Unmarshal(d.Body, &evt); err != nil {
					t.Fatalf("failed to unmarshal call event: %v", err)
				}
				if evt.Type == call.EventIncoming {
					incoming = evt
					gotIncoming = true
				}
			case gatewayamqp.InboundRoutingKey:
				if err := json.Unmarshal(d.Body, &inbound); err != nil {
					t.Fatalf("failed to unmarshal inbound call event: %v", err)
				}
				gotInbound = true
			}
		case <-deadline:
			t.Fatalf("timed out waiting for the incoming call event (got it: %v) and the inbound call event (got it: %v)", gotIncoming, gotInbound)
		}
	}
	return incoming, inbound
}

// TestGatewayInboundCallRecordsAndPublishes drives a whole inbound call through
// a running gateway: the call arrives, the backend answers it over
// gateway.call, the peer's audio is captured, the call ends, and the recording
// has to be in the bucket with its key on the ended event.
func TestGatewayInboundCallRecordsAndPublishes(t *testing.T) {
	ctx := context.Background()

	conn := startRabbitMQ(t)
	redisClient := startRedis(t)

	minioContainer, err := minio.Run(ctx, minioImage)
	if err != nil {
		t.Fatalf("failed to start minio container: %v", err)
	}
	t.Cleanup(func() {
		if err := minioContainer.Terminate(context.Background()); err != nil {
			t.Errorf("failed to terminate minio container: %v", err)
		}
	})
	endpoint, err := minioContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("failed to get minio connection string: %v", err)
	}

	const bucket = "vectax-calls-e2e"
	rawS3, err := rawS3Client(ctx, endpoint, minioContainer.Username, minioContainer.Password)
	if err != nil {
		t.Fatalf("failed to build raw s3 client: %v", err)
	}
	if _, err := rawS3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}
	mediaStore, err := media.NewS3Store(ctx, media.S3Config{
		Bucket:          bucket,
		Region:          "us-east-1",
		Endpoint:        "http://" + endpoint,
		AccessKeyID:     minioContainer.Username,
		SecretAccessKey: minioContainer.Password,
	})
	if err != nil {
		t.Fatalf("NewS3Store failed: %v", err)
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

	caller := &fakeCaller{}
	fake := newFakeWAClient()
	fake.caller = caller
	mgr := session.NewManager(func(string, *types.JID) (session.WAClient, error) {
		return fake, nil
	})

	waLogger, logger := logging.New()
	dsn := startPostgresForGateway(t)
	if _, err := store.Open(ctx, dsn, waLogger); err != nil {
		t.Fatalf("store.Open failed: %v", err)
	}
	dedupeStore, err := dedupe.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("dedupe.Open failed: %v", err)
	}
	t.Cleanup(dedupeStore.Close)
	registryStore, err := registry.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("registry.Open failed: %v", err)
	}
	t.Cleanup(registryStore.Close)

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
	for _, rk := range []string{gatewayamqp.CallRoutingKey, gatewayamqp.ChannelStatusRoutingKey, gatewayamqp.InboundRoutingKey} {
		if err := probeCh.QueueBind(probeQ.Name, rk, gatewayamqp.EventsExchange, false, nil); err != nil {
			t.Fatalf("failed to bind probe queue to %s: %v", rk, err)
		}
	}
	deliveries, err := probeCh.Consume(probeQ.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to consume probe queue: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- gateway.Run(runCtx, gateway.Deps{
			Consumer:             consumer,
			Publisher:            publisher,
			Manager:              mgr,
			Ownership:            ownership.NewStore(redisClient, 4),
			Dedupe:               dedupeStore,
			Registry:             registryStore,
			MediaStore:           mediaStore,
			InstanceID:           "gateway-call-e2e",
			ShardLockTTL:         30 * time.Second,
			ShutdownDrainTimeout: 10 * time.Second,
			CallOptions: call.Options{
				TmpDir: t.TempDir(),
				Record: true,
			},
			Logger: logger,
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-runErrCh:
		case <-time.After(20 * time.Second):
			t.Error("gateway.Run did not return after cancellation")
		}
	})

	const channelID = "channel-call-e2e"
	const tenantID = "tenant-call-e2e"

	// Pair, so the channel has a live session with the caller attached.
	fake.qrItems = []whatsmeow.QRChannelItem{whatsmeow.QRChannelSuccess}
	pairBody, err := json.Marshal(gatewayamqp.PairCommand{
		TenantID: tenantID, ChannelID: channelID, UserID: "user-call-e2e",
	})
	if err != nil {
		t.Fatalf("failed to marshal pair command: %v", err)
	}
	publishCtx, publishCancel := context.WithTimeout(ctx, 15*time.Second)
	defer publishCancel()
	if err := probeCh.PublishWithContext(publishCtx, gatewayamqp.GatewayPairExchange, "0", false, false, rabbitmq.Publishing{
		ContentType: "application/json",
		Body:        pairBody,
	}); err != nil {
		t.Fatalf("failed to publish pair command: %v", err)
	}

	waitFor(t, 20*time.Second, "the incoming-call handler to be registered", func() bool {
		return caller.incomingHandler() != nil
	})

	lc := &fakeLiveCall{callID: "CALL1", peer: "5511888888888@s.whatsapp.net"}
	caller.fireIncoming(lc)

	// The arriving call publishes two deliveries on two routing keys: the
	// call.Event on CallRoutingKey, and the inbound-message-shaped
	// call.InboundCallEvent on InboundRoutingKey (see call.NewInboundCallEvent),
	// which is how the backend's message pipeline creates the chat row. Both
	// have to be drained from one loop over the shared probe channel --
	// waitForCallEvent alone would silently discard whichever one it isn't
	// looking for, since a channel read can't be put back.
	incoming, inboundCall := waitForIncomingCallAndInboundEvent(t, deliveries, 15*time.Second)
	if incoming.CallID != "CALL1" || incoming.Direction != call.DirectionInbound {
		t.Fatalf("incoming event = %+v, want inbound CALL1", incoming)
	}
	if incoming.TenantID != tenantID || incoming.ChannelID != channelID {
		t.Fatalf("incoming identity = %+v, want %s/%s", incoming, tenantID, channelID)
	}
	// PhoneNumberID must be the channel id, not the device JID the fake pair
	// establishes ("15550000000" -- see fakeWAClient.QRChannel): the backend
	// looks a gateway channel up by that field holding the channel's own id,
	// so a device-JID value would make it match no channel and the event
	// would be dropped, exactly as it was on the real call this test guards.
	if incoming.PhoneNumberID != channelID {
		t.Fatalf("incoming PhoneNumberID = %q, want the channel id %q", incoming.PhoneNumberID, channelID)
	}
	// From is the resolved phone-number user, matching the shape an inbound
	// message's From takes -- not the raw peer JID string.
	if incoming.From != "5511888888888" {
		t.Errorf("From = %q, want the resolved phone number", incoming.From)
	}

	// This is the exact delivery the reported bug dropped: the backend's
	// ConsumeInbound logged "no channel found for phoneNumberId ..., dropping
	// inbound message" for it, because PhoneNumberID carried the device's
	// phone number instead of the channel id.
	if inboundCall.PhoneNumberID != channelID {
		t.Fatalf("inbound call PhoneNumberID = %q, want the channel id %q", inboundCall.PhoneNumberID, channelID)
	}
	if inboundCall.ProviderMessageID != "CALL1" {
		t.Fatalf("inbound call ProviderMessageID = %q, want CALL1", inboundCall.ProviderMessageID)
	}

	answerBody, err := json.Marshal(gatewayamqp.GatewayCallCommand{
		TenantID: tenantID, ChannelID: channelID, CommandID: "cmd-answer", CallID: "CALL1", Action: "answer",
	})
	if err != nil {
		t.Fatalf("failed to marshal answer command: %v", err)
	}
	if err := probeCh.PublishWithContext(publishCtx, gatewayamqp.GatewayCallExchange, "0", false, false, rabbitmq.Publishing{
		ContentType: "application/json",
		Body:        answerBody,
	}); err != nil {
		t.Fatalf("failed to publish answer command: %v", err)
	}

	ack := waitForCallEvent(t, deliveries, call.EventCommandAck, 15*time.Second)
	if ack.CommandID != "cmd-answer" {
		t.Fatalf("ack = %+v, want cmd-answer", ack)
	}
	if actions := lc.recordedActions(); len(actions) != 1 || actions[0] != "answer" {
		t.Fatalf("call actions = %v, want [answer]", actions)
	}

	lc.fireReady()
	lc.feedAudio([]float32{0.5, -0.5, 0.25})
	lc.fireEnd("hangup")

	ended := waitForCallEvent(t, deliveries, call.EventEnded, 15*time.Second)
	if ended.Reason != "hangup" {
		t.Fatalf("ended reason = %q, want hangup", ended.Reason)
	}
	if ended.Media != nil {
		t.Fatalf("ended media = %+v, want nil: the recording arrives on its own event", ended.Media)
	}

	// The upload runs off the call's teardown path, so the recording's keys
	// arrive on a later, separate event rather than on ended itself. All three
	// audio tracks ride on that one event: the mix the chat plays, plus one per
	// side for transcription to attribute.
	recording := waitForCallEvent(t, deliveries, call.EventRecording, 15*time.Second)
	tracks := []struct {
		field string
		media *call.Media
		key   string
	}{
		{"media", recording.Media, "calls/" + channelID + "/CALL1.wav"},
		{"peerMedia", recording.PeerMedia, "calls/" + channelID + "/CALL1-peer.wav"},
		{"operatorMedia", recording.OperatorMedia, "calls/" + channelID + "/CALL1-operator.wav"},
	}
	for _, track := range tracks {
		if track.media == nil || track.media.Key != track.key {
			t.Fatalf("recording %s = %+v, want key %s", track.field, track.media, track.key)
		}
		if track.media.MimeType != "audio/wav" {
			t.Fatalf("recording %s mime = %q, want audio/wav", track.field, track.media.MimeType)
		}
		assertWAVInBucket(ctx, t, rawS3, bucket, track.key)
	}
}

// assertWAVInBucket reads an uploaded recording back and checks it is a real
// RIFF/WAVE file rather than an empty or half-written object.
func assertWAVInBucket(ctx context.Context, t *testing.T, client *s3.Client, bucket, key string) {
	t.Helper()

	obj, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("%s was not uploaded: %v", key, err)
	}
	defer func() {
		if err := obj.Body.Close(); err != nil {
			t.Errorf("failed to close object body: %v", err)
		}
	}()
	header := make([]byte, 12)
	if _, err := io.ReadFull(obj.Body, header); err != nil {
		t.Fatalf("failed to read the header of %s: %v", key, err)
	}
	if !bytes.Equal(header[0:4], []byte("RIFF")) || !bytes.Equal(header[8:12], []byte("WAVE")) {
		t.Fatalf("%s header = %q, want a RIFF/WAVE header", key, header)
	}
}
