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

	m.WaitForRecordings(2 * time.Second)

	if _, ok := store.object("calls/chan-a/C1.wav"); !ok {
		t.Error("the recorder got nothing while the stream was attached")
	}
}

func TestStreamWorksWithRecordingDisabled(t *testing.T) {
	store := newMemStore()
	m := call.NewManager(&memPublisher{}, store,
		func(string) call.Identity { return call.Identity{TenantID: "t1"} },
		nil,
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

	src := lc.playedSrc()
	if src == nil {
		t.Fatal("Track did not subscribe the call's outbound audio")
	}

	got := make([]byte, len(want))
	if _, err := io.ReadFull(src, got); err != nil {
		t.Fatalf("read operator audio: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("got = %v, want %v", got, want)
	}
}

func TestAttachRacingTeardownNeverStrandsAStream(t *testing.T) {
	for i := 0; i < 200; i++ {
		m := newTestManager(t, &memPublisher{}, newMemStore(), time.Now)
		caller := &fakeCaller{}
		m.Attach("chan-a", caller)

		lc := &fakeCall{id: "C1"}
		caller.fireIncoming(lc)

		attached := make(chan *call.Stream, 1)
		go func() {
			stream, _, ok := m.AttachStream("chan-a", "C1")
			if !ok {
				attached <- nil
				return
			}
			attached <- stream
		}()
		lc.fireEnd("hangup")

		stream := <-attached
		if stream == nil {
			continue
		}
		select {
		case _, open := <-stream.Audio():
			if open {
				t.Fatal("the attached stream delivered a frame on an ended call")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("AttachStream handed back a stream on an ended call and nothing ever closed it: " +
				"the operator would wait forever for the end frame")
		}
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
