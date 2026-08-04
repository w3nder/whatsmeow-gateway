package call_test

import (
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/w3nder/whatsmeow-gateway/internal/call"
)

func TestPeerAudioReachesBothRecorderAndStream(t *testing.T) {
	pub := &memPublisher{}
	store := newMemStore()
	m := newTestManager(t, pub, store, time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	lc := &fakeCall{id: "C1"}
	caller.fireIncoming(lc)

	stream, detach, ok := m.AttachStream("chan-a", "C1")
	if !ok {
		t.Fatal("AttachStream returned false for a live call")
	}
	defer detach()

	lc.feedAudio([]float32{0.5, -0.5})

	select {
	case frame := <-stream.Audio():
		if len(frame) != 2 || frame[0] != 0.5 {
			t.Errorf("frame = %v, want the peer frame", frame)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the peer frame on the stream")
	}

	lc.fireEnd("hangup")

	// The upload now runs off the call's teardown path; wait for it rather
	// than racing the assertion against the background goroutine.
	m.WaitForRecordings(2 * time.Second)

	if _, ok := store.object("calls/chan-a/C1.wav"); !ok {
		t.Error("the recorder got nothing while the stream was attached")
	}
}

// The stream must work with recording off: the two sinks are independent,
// and a call with Record: false must still let an operator listen in.
func TestStreamWorksWithRecordingDisabled(t *testing.T) {
	store := newMemStore()
	m := call.NewManager(&memPublisher{}, store,
		func(string) call.Identity { return call.Identity{TenantID: "t1"} },
		nil,
		call.Options{TmpDir: t.TempDir(), Record: false, Now: time.Now},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	lc := &fakeCall{id: "C1"}
	caller.fireIncoming(lc)

	stream, detach, ok := m.AttachStream("chan-a", "C1")
	if !ok {
		t.Fatal("AttachStream returned false for a live call")
	}
	defer detach()

	lc.feedAudio([]float32{0.25})

	select {
	case frame := <-stream.Audio():
		if len(frame) != 1 || frame[0] != 0.25 {
			t.Errorf("frame = %v, want the peer frame", frame)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the peer frame on the stream")
	}

	lc.fireEnd("hangup")

	if store.objectCount() != 0 {
		t.Errorf("uploaded %d objects with recording off, want none", store.objectCount())
	}
}

// WriteAudio must be verified end to end: newStream calls lc.Play at
// construction, so asserting only that "play" was recorded would pass even
// if WriteAudio silently dropped every frame. Reading the bytes back off the
// pipe is what actually exercises the write path.
func TestOperatorAudioReachesTheCall(t *testing.T) {
	m := newTestManager(t, &memPublisher{}, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	lc := &fakeCall{id: "C1"}
	caller.fireIncoming(lc)

	stream, detach, _ := m.AttachStream("chan-a", "C1")
	defer detach()

	want := []byte{0x00, 0x40, 0x00, 0xC0}
	if err := stream.WriteAudio(want); err != nil {
		t.Fatalf("WriteAudio: %v", err)
	}

	// newStream calls Play from a goroutine of its own, so it may not have
	// run yet; poll rather than assume it already has.
	var src io.ReadCloser
	deadline := time.Now().Add(time.Second)
	for src == nil && time.Now().Before(deadline) {
		src = lc.playedSrc()
		if src == nil {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if src == nil {
		t.Fatal("Play was never called")
	}

	got := make([]byte, len(want))
	readDone := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(src, got)
		readDone <- err
	}()

	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("read operator audio: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out reading the bytes WriteAudio queued")
	}

	if string(got) != string(want) {
		t.Errorf("got = %v, want %v", got, want)
	}
}

func TestOperatorVideoReachesTheCall(t *testing.T) {
	m := newTestManager(t, &memPublisher{}, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	lc := &fakeCall{id: "C1"}
	caller.fireIncoming(lc)

	stream, detach, _ := m.AttachStream("chan-a", "C1")
	defer detach()

	unit := []byte{0, 0, 0, 1, 0x65, 0xAA}
	if err := stream.WriteVideo(unit); err != nil {
		t.Fatalf("WriteVideo: %v", err)
	}

	sent := lc.sentVideo()
	if len(sent) != 1 || string(sent[0]) != string(unit) {
		t.Errorf("sent = %v, want the access unit", sent)
	}
}

// The peer's own request for a fresh IDR (WhatsApp's PLI/FIR feedback after
// packet loss) has to reach the attached stream's Keyframe channel, same as
// the one-shot request on attach -- otherwise a peer that lost packets mid
// call never gets rescued.
func TestPeerKeyframeRequestReachesTheAttachedStream(t *testing.T) {
	m := newTestManager(t, &memPublisher{}, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	lc := &fakeCall{id: "C1"}
	caller.fireIncoming(lc)

	stream, detach, ok := m.AttachStream("chan-a", "C1")
	if !ok {
		t.Fatal("AttachStream returned false for a live call")
	}
	defer detach()

	// Drain the one-shot request AttachStream itself makes on attach, so the
	// assertion below can only pass because of fireVideoKeyframeRequest.
	select {
	case <-stream.Keyframe():
	case <-time.After(time.Second):
		t.Fatal("timed out draining the attach-time keyframe request")
	}

	lc.fireVideoKeyframeRequest()

	select {
	case <-stream.Keyframe():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the peer's keyframe request on the stream")
	}
}

// With no operator attached there is nothing to route a keyframe request to;
// the callback must simply do nothing rather than panic on a nil stream.
func TestPeerKeyframeRequestWithNoStreamAttachedIsANoop(t *testing.T) {
	m := newTestManager(t, &memPublisher{}, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	lc := &fakeCall{id: "C1"}
	caller.fireIncoming(lc)

	lc.fireVideoKeyframeRequest()
}

func TestAttachStreamOnAnUnknownCallFails(t *testing.T) {
	m := newTestManager(t, &memPublisher{}, newMemStore(), time.Now)
	if _, _, ok := m.AttachStream("chan-a", "NOPE"); ok {
		t.Error("AttachStream returned true for a call that does not exist")
	}
}

// A second attach must not leave two consumers racing for the same frames:
// the first stream is closed and the second one takes over.
func TestAttachStreamReplacesAnEarlierStream(t *testing.T) {
	m := newTestManager(t, &memPublisher{}, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	lc := &fakeCall{id: "C1"}
	caller.fireIncoming(lc)

	first, _, ok := m.AttachStream("chan-a", "C1")
	if !ok {
		t.Fatal("AttachStream returned false for a live call")
	}

	second, detach, ok := m.AttachStream("chan-a", "C1")
	if !ok {
		t.Fatal("AttachStream returned false for a live call")
	}
	defer detach()

	select {
	case _, ok := <-first.Audio():
		if ok {
			t.Error("the replaced stream delivered a frame, want its channel closed")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the replaced stream's channel to close")
	}

	lc.feedAudio([]float32{0.75})

	select {
	case frame := <-second.Audio():
		if len(frame) != 1 || frame[0] != 0.75 {
			t.Errorf("frame = %v, want the peer frame", frame)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the peer frame on the replacing stream")
	}
}

// detach can run more than once -- a caller's defer alongside a replacing
// AttachStream, say -- and must not panic.
func TestDetachIsIdempotent(t *testing.T) {
	m := newTestManager(t, &memPublisher{}, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	lc := &fakeCall{id: "C1"}
	caller.fireIncoming(lc)

	_, detach, ok := m.AttachStream("chan-a", "C1")
	if !ok {
		t.Fatal("AttachStream returned false for a live call")
	}

	detach()
	detach()
}

// Nothing else tells an operator that the call ended; the stream itself must
// close so a consumer parked on Audio() is not left hanging forever.
func TestStreamClosesWhenTheCallEnds(t *testing.T) {
	m := newTestManager(t, &memPublisher{}, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	lc := &fakeCall{id: "C1"}
	caller.fireIncoming(lc)

	stream, detach, ok := m.AttachStream("chan-a", "C1")
	if !ok {
		t.Fatal("AttachStream returned false for a live call")
	}
	defer detach()

	lc.fireEnd("hangup")

	// finish runs synchronously from fireEnd, so the close is visible
	// immediately -- no polling needed.
	select {
	case _, ok := <-stream.Audio():
		if ok {
			t.Error("stream.Audio() delivered a frame after the call ended, want the channel closed")
		}
	default:
		t.Error("stream.Audio() was not closed when the call ended")
	}

	if err := stream.WriteAudio([]byte{0, 0}); err == nil {
		t.Error("WriteAudio succeeded after the call ended, want an error")
	}
}

// A slow or gone operator must never block the media goroutine that feeds the
// recorder, or the recording stalls with it.
func TestStreamDropsFramesWhenTheOperatorIsNotReading(t *testing.T) {
	m := newTestManager(t, &memPublisher{}, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	lc := &fakeCall{id: "C1"}
	caller.fireIncoming(lc)

	stream, detach, _ := m.AttachStream("chan-a", "C1")
	defer detach()
	_ = stream

	done := make(chan struct{})
	go func() {
		for i := 0; i < 10_000; i++ {
			lc.feedAudio([]float32{0.1})
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("feeding audio blocked on an unread stream")
	}
}

// The mu fence in close() exists to protect exactly this: a stream torn
// down while both directions are actively flowing, so a fenceless close
// would either panic on a send to a closed channel or deadlock the media
// goroutine behind close's pending Lock (see the review that caught
// WriteVideo doing precisely that before it snapshotted closed and
// released the lock). Run under -race and with -count>1, this is where
// that fencing actually gets exercised instead of merely inspected.
//
// teardown runs concurrently with traffic in both directions and reports
// whether the stream should still be usable afterward: detach makes it
// dead, but ending some *other* call must leave this stream's traffic
// untouched.
func raceTrafficAgainstTeardown(t *testing.T, teardown func(lc *fakeCall, detach func())) {
	t.Helper()
	m := newTestManager(t, &memPublisher{}, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	lc := &fakeCall{id: "C1"}
	caller.fireIncoming(lc)

	stream, detach, ok := m.AttachStream("chan-a", "C1")
	if !ok {
		t.Fatal("AttachStream returned false for a live call")
	}
	defer detach()

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Stands in for the calling library's media goroutine: the one this
	// whole package exists to keep from ever blocking or panicking.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			lc.feedAudio([]float32{float32(i%3) * 0.1})
			lc.feedVideo([]byte{0, 0, 0, 1, 0x65, byte(i)})
		}
	}()

	// The operator's own traffic, flowing the other way at the same time,
	// straight through the same Stream that close() is about to tear down.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			_ = stream.WriteAudio([]byte{byte(i), byte(i >> 8)})
			_ = stream.WriteVideo([]byte{0, 0, 0, 1, 0x65, byte(i)})
		}
	}()

	// Let real interleaving build up before tearing down mid-flight.
	time.Sleep(5 * time.Millisecond)
	teardown(lc, detach)

	close(stop)

	waitDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
	case <-time.After(5 * time.Second):
		t.Fatal("traffic goroutines did not stop after teardown")
	}

	// Whatever tore it down, the stream itself is now dead: further writes
	// must fail fast rather than block or silently succeed.
	if err := stream.WriteAudio([]byte{0, 0}); err == nil {
		t.Error("WriteAudio succeeded after teardown, want an error")
	}
	if err := stream.WriteVideo([]byte{0, 0, 0, 1, 0x65}); err == nil {
		t.Error("WriteVideo succeeded after teardown, want an error")
	}
}

func TestConcurrentTrafficSurvivesDetach(t *testing.T) {
	raceTrafficAgainstTeardown(t, func(_ *fakeCall, detach func()) {
		detach()
	})
}

func TestConcurrentTrafficSurvivesCallEnding(t *testing.T) {
	raceTrafficAgainstTeardown(t, func(lc *fakeCall, _ func()) {
		lc.fireEnd("hangup")
	})
}

// Same guarantee as above, for the video sink: dropping is what protects the
// media goroutine, and only the audio direction was covered before.
func TestStreamDropsVideoFramesWhenTheOperatorIsNotReading(t *testing.T) {
	m := newTestManager(t, &memPublisher{}, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	lc := &fakeCall{id: "C1"}
	caller.fireIncoming(lc)

	stream, detach, _ := m.AttachStream("chan-a", "C1")
	defer detach()
	_ = stream

	done := make(chan struct{})
	go func() {
		for i := 0; i < 10_000; i++ {
			lc.feedVideo([]byte{0, 0, 0, 1, 0x65, byte(i)})
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("feeding video blocked on an unread stream")
	}
}
