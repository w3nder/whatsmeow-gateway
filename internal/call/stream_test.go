package call_test

import (
	"testing"
	"time"
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

	if _, ok := store.object("calls/chan-a/C1.wav"); !ok {
		t.Error("the recorder got nothing while the stream was attached")
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

	if err := stream.WriteAudio([]byte{0x00, 0x40, 0x00, 0xC0}); err != nil {
		t.Fatalf("WriteAudio: %v", err)
	}

	waitFor(t, time.Second, "Play to be wired", func() bool {
		for _, action := range lc.recordedActions() {
			if action == "play" {
				return true
			}
		}
		return false
	})
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

func TestAttachStreamOnAnUnknownCallFails(t *testing.T) {
	m := newTestManager(t, &memPublisher{}, newMemStore(), time.Now)
	if _, _, ok := m.AttachStream("chan-a", "NOPE"); ok {
		t.Error("AttachStream returned true for a call that does not exist")
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

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
