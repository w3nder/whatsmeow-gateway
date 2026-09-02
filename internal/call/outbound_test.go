package call_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/purpshell/meowcaller"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/call"
)

const readFrameBudget = 500 * time.Millisecond

func readFrame(t *testing.T, src meowcaller.AudioSource) []float32 {
	t.Helper()

	type result struct {
		frame []float32
		err   error
	}
	done := make(chan result, 1)
	go func() {
		frame, err := src.ReadFrame()
		done <- result{frame, err}
	}()

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("ReadFrame: %v -- an error ends playback, and the call goes mute", got.err)
		}
		if len(got.frame) != meowcaller.FrameSamples {
			t.Fatalf("ReadFrame returned %d samples, want %d", len(got.frame), meowcaller.FrameSamples)
		}
		return got.frame
	case <-time.After(readFrameBudget):
		t.Fatalf("ReadFrame blocked for %s: the library's send loop is parked in our source, "+
			"so the gateway has stopped emitting RTP entirely", readFrameBudget)
		return nil
	}
}

func isSilence(frame []float32) bool {
	for _, sample := range frame {
		if sample != 0 {
			return false
		}
	}
	return true
}

func trackedCall(t *testing.T) (*call.Manager, *fakeCall, meowcaller.AudioSource) {
	t.Helper()
	m, lc, src, _ := trackedRecordedCall(t)
	return m, lc, src
}

func trackedRecordedCall(t *testing.T) (*call.Manager, *fakeCall, meowcaller.AudioSource, *memStore) {
	t.Helper()

	store := newMemStore()
	m := newTestManager(t, &memPublisher{}, store, time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	lc := &fakeCall{id: "C1"}
	caller.fireIncoming(lc)

	src := lc.playedSrc()
	if src == nil {
		t.Fatal("Track did not subscribe the call's outbound audio")
	}
	return m, lc, meowcaller.PCMStream(src), store
}

func TestSilentAttachedOperatorDoesNotStallTheSendLoop(t *testing.T) {
	m, _, source := trackedCall(t)

	if frame := readFrame(t, source); !isSilence(frame) {
		t.Error("an idle call transmitted something other than silence")
	}

	_, detach, ok := m.AttachStream("chan-a", "C1")
	if !ok {
		t.Fatal("AttachStream returned false for a live call")
	}
	defer detach()

	for i := 0; i < 5; i++ {
		if frame := readFrame(t, source); !isSilence(frame) {
			t.Errorf("frame %d from a silent operator was not silence", i)
		}
	}

	detach()
	if frame := readFrame(t, source); !isSilence(frame) {
		t.Error("the call stopped transmitting silence after the operator detached")
	}
}

func TestDetachDropsTheOperatorsQueuedAudio(t *testing.T) {
	m, _, source := trackedCall(t)

	stream, detach, ok := m.AttachStream("chan-a", "C1")
	if !ok {
		t.Fatal("AttachStream returned false for a live call")
	}

	loud := make([]byte, meowcaller.FrameSamples*2)
	for i := 0; i < len(loud); i += 2 {
		loud[i], loud[i+1] = 0x00, 0x40
	}
	if err := stream.WriteAudio(loud); err != nil {
		t.Fatalf("WriteAudio: %v", err)
	}

	detach()

	if frame := readFrame(t, source); !isSilence(frame) {
		t.Error("the detached operator's queued audio was still transmitted")
	}
}

func TestOperatorAudioIsTransmittedThenGivesWayToSilence(t *testing.T) {
	m, _, source := trackedCall(t)

	stream, detach, ok := m.AttachStream("chan-a", "C1")
	if !ok {
		t.Fatal("AttachStream returned false for a live call")
	}
	defer detach()

	loud := make([]byte, meowcaller.FrameSamples*2)
	for i := 0; i < len(loud); i += 2 {
		loud[i], loud[i+1] = 0x00, 0x40
	}
	if err := stream.WriteAudio(loud); err != nil {
		t.Fatalf("WriteAudio: %v", err)
	}

	frame := readFrame(t, source)
	if frame[0] < 0.49 || frame[0] > 0.51 {
		t.Errorf("first sample = %v, want the operator's 0.5", frame[0])
	}

	if next := readFrame(t, source); !isSilence(next) {
		t.Error("the frame after the operator went quiet was not silence")
	}
}

func TestAnnouncementTakesTheFloorAndGivesItBack(t *testing.T) {
	m, lc, source := trackedCall(t)

	stream, detach, ok := m.AttachStream("chan-a", "C1")
	if !ok {
		t.Fatal("AttachStream returned false for a live call")
	}
	defer detach()

	announcement := make([]byte, meowcaller.FrameSamples*2)
	for i := 0; i < len(announcement); i += 2 {
		announcement[i], announcement[i+1] = 0x00, 0x20
	}
	fetch := func(context.Context, string) ([]byte, error) { return announcement, nil }
	if err := m.Dispatch(context.Background(), &fakeCaller{}, amqp.GatewayCallCommand{
		ChannelID: "chan-a", CallID: "C1", CommandID: "cmd-1",
		Action: "play", MediaURL: "https://example.test/hold.pcm",
	}, fetch); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	if count := lc.playCount(); count != 1 {
		t.Errorf("Play was called %d times, want exactly one subscribe for the whole call", count)
	}

	frame := readFrame(t, source)
	if frame[0] < 0.24 || frame[0] > 0.26 {
		t.Errorf("first sample = %v, want the announcement's 0.25", frame[0])
	}

	loud := make([]byte, meowcaller.FrameSamples*2)
	for i := 0; i < len(loud); i += 2 {
		loud[i], loud[i+1] = 0x00, 0x40
	}
	if err := stream.WriteAudio(loud); err != nil {
		t.Fatalf("WriteAudio after the announcement: %v", err)
	}
	frame = readFrame(t, source)
	if frame[0] < 0.49 || frame[0] > 0.51 {
		t.Errorf("first sample = %v, want the operator's 0.5 back after the announcement", frame[0])
	}
}

func TestAttachingMidAnnouncementDoesNotTruncateIt(t *testing.T) {
	m, _, source := trackedCall(t)

	announcement := make([]byte, meowcaller.FrameSamples*2*3)
	for i := 0; i < len(announcement); i += 2 {
		announcement[i], announcement[i+1] = 0x00, 0x20
	}
	fetch := func(context.Context, string) ([]byte, error) { return announcement, nil }
	if err := m.Dispatch(context.Background(), &fakeCaller{}, amqp.GatewayCallCommand{
		ChannelID: "chan-a", CallID: "C1", Action: "play", MediaURL: "https://example.test/hold.pcm",
	}, fetch); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	if frame := readFrame(t, source); isSilence(frame) {
		t.Fatal("the announcement's first frame was silence")
	}

	_, detach, ok := m.AttachStream("chan-a", "C1")
	if !ok {
		t.Fatal("AttachStream returned false for a live call")
	}
	defer detach()

	for i := 1; i < 3; i++ {
		if frame := readFrame(t, source); isSilence(frame) {
			t.Errorf("announcement frame %d was silence: the attach truncated it", i)
		}
	}
}

func TestWhatTheSourceTransmitsLandsOnTheOperatorTrack(t *testing.T) {
	m, lc, source, store := trackedRecordedCall(t)

	stream, detach, ok := m.AttachStream("chan-a", "C1")
	if !ok {
		t.Fatal("AttachStream returned false for a live call")
	}
	defer detach()

	loud := make([]byte, meowcaller.FrameSamples*2)
	for i := 0; i < len(loud); i += 2 {
		loud[i], loud[i+1] = 0x00, 0x40
	}
	if err := stream.WriteAudio(loud); err != nil {
		t.Fatalf("WriteAudio: %v", err)
	}
	readFrame(t, source)

	lc.fireEnd("hangup")
	m.WaitForRecordings(5 * time.Second)

	operator, ok := store.object("calls/chan-a/C1-operator.wav")
	if !ok {
		t.Fatal("the operator track was never uploaded")
	}
	if got := wavSample(operator, 0); got < 16382 || got > 16384 {
		t.Errorf("operator track sample = %d, want the transmitted 0.5", got)
	}

	peer, ok := store.object("calls/chan-a/C1-peer.wav")
	if !ok {
		t.Fatal("the peer track was never uploaded")
	}
	if data := peer[44:]; !bytes.Equal(data, make([]byte, len(data))) {
		t.Error("the operator's audio bled into the customer's track")
	}
}

func TestTransmittedSilenceDoesNotCreateARecording(t *testing.T) {
	m, lc, source, store := trackedRecordedCall(t)

	for i := 0; i < 5; i++ {
		if frame := readFrame(t, source); !isSilence(frame) {
			t.Fatalf("frame %d of an idle call was not silence", i)
		}
	}

	lc.fireEnd("hangup")
	m.WaitForRecordings(5 * time.Second)

	if got := store.objectCount(); got != 0 {
		t.Errorf("uploaded %d objects for a call with no audio, want none", got)
	}
}

func TestOutboundAudioEndsWhenTheCallDoes(t *testing.T) {
	_, lc, source := trackedCall(t)

	lc.fireEnd("hangup")

	if _, err := source.ReadFrame(); err != io.EOF {
		t.Errorf("ReadFrame after the call ended = %v, want io.EOF", err)
	}
}

func TestAnnouncementIsPacedByTheReader(t *testing.T) {
	m, _, source := trackedCall(t)

	announcement := bytes.Repeat([]byte{0x00, 0x20}, meowcaller.FrameSamples*4)
	fetch := func(context.Context, string) ([]byte, error) { return announcement, nil }
	if err := m.Dispatch(context.Background(), &fakeCaller{}, amqp.GatewayCallCommand{
		ChannelID: "chan-a", CallID: "C1", Action: "play", MediaURL: "https://example.test/hold.pcm",
	}, fetch); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	for i := 0; i < 4; i++ {
		if frame := readFrame(t, source); isSilence(frame) {
			t.Fatalf("announcement frame %d was silence: it ran out early", i)
		}
	}
	if frame := readFrame(t, source); !isSilence(frame) {
		t.Error("the frame after the announcement was not silence")
	}
}
