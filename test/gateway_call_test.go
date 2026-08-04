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
	for _, rk := range []string{gatewayamqp.CallRoutingKey, gatewayamqp.ChannelStatusRoutingKey} {
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

	incoming := waitForCallEvent(t, deliveries, call.EventIncoming, 15*time.Second)
	if incoming.CallID != "CALL1" || incoming.Direction != call.DirectionInbound {
		t.Fatalf("incoming event = %+v, want inbound CALL1", incoming)
	}
	if incoming.TenantID != tenantID || incoming.ChannelID != channelID {
		t.Fatalf("incoming identity = %+v, want %s/%s", incoming, tenantID, channelID)
	}
	if incoming.From != "5511888888888@s.whatsapp.net" {
		t.Errorf("From = %q, want the peer jid", incoming.From)
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
	wantKey := "calls/" + channelID + "/CALL1.wav"
	if ended.Media == nil || ended.Media.Key != wantKey {
		t.Fatalf("ended media = %+v, want key %s", ended.Media, wantKey)
	}

	obj, err := rawS3.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(wantKey)})
	if err != nil {
		t.Fatalf("recording was not uploaded: %v", err)
	}
	defer func() {
		if err := obj.Body.Close(); err != nil {
			t.Errorf("failed to close object body: %v", err)
		}
	}()
	header := make([]byte, 12)
	if _, err := io.ReadFull(obj.Body, header); err != nil {
		t.Fatalf("failed to read the recording header: %v", err)
	}
	if !bytes.Equal(header[0:4], []byte("RIFF")) || !bytes.Equal(header[8:12], []byte("WAVE")) {
		t.Fatalf("recording header = %q, want a RIFF/WAVE header", header)
	}
}
