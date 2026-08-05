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

// This is the one test file that reaches for the calling library's own types,
// and it does so deliberately. Everything else in the package programs against
// the port in port.go, which is exactly why the port could not catch the bug
// these tests pin down: the fake's Play returns immediately, while the real
// library's transmit loop pulls from the source it was given, synchronously,
// once per frame interval. Only the library's real AudioSource can prove the
// source we hand it never parks that loop.
//
// meowcaller.Player.nextFrame -- the send loop's actual entry point -- is
// unexported, so these tests call the ReadFrame under it. That is the whole of
// what nextFrame does with a playing source (engine_media.go's send loop calls
// player.nextFrame(), which calls src.ReadFrame() and only substitutes silence
// when it errors), so a ReadFrame that returns a frame promptly and without an
// error is precisely the condition the loop needs to keep emitting RTP.

// readFrameBudget is how long a ReadFrame may take before the test calls it a
// stall. The send loop's cadence is FrameSamples at SampleRate -- 60 ms -- so
// this is already several frames' worth of missing RTP; the real source
// returns in nanoseconds, and the broken one never returned at all.
const readFrameBudget = 500 * time.Millisecond

// readFrame runs one ReadFrame under a deadline, so a source that blocks fails
// the test instead of hanging it.
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

// trackedCall brings up a manager with one live call and returns the source
// the gateway subscribed to it, wrapped the way the library wraps it.
func trackedCall(t *testing.T) (*call.Manager, *fakeCall, meowcaller.AudioSource) {
	t.Helper()
	m, lc, src, _ := trackedRecordedCall(t)
	return m, lc, src
}

// trackedRecordedCall is trackedCall with the recording's object store handed
// back too, for the tests that follow what the source transmits all the way
// onto the recording.
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

// The failure this whole change exists for: an operator attaches to listen and
// never sends a byte -- no microphone permission, muted, a viewer that only
// renders the peer's video. That must look, to the library's send loop,
// exactly like a call with no operator at all. Before the fix the source was
// an io.Pipe with no writer, so the first ReadFrame after the attach blocked
// forever, the send loop stopped emitting, and WhatsApp's relay stopped
// bridging the peer's media back -- which flatlined the recording too.
func TestSilentAttachedOperatorDoesNotStallTheSendLoop(t *testing.T) {
	m, _, source := trackedCall(t)

	// A frame before the attach, so the assertions after it cannot pass just
	// because the call was never transmitting in the first place.
	if frame := readFrame(t, source); !isSilence(frame) {
		t.Error("an idle call transmitted something other than silence")
	}

	_, detach, ok := m.AttachStream("chan-a", "C1")
	if !ok {
		t.Fatal("AttachStream returned false for a live call")
	}
	defer detach()

	// Several ticks' worth: the old failure was permanent, so one is enough to
	// catch it, but a source that only survives its first read would be worse
	// than useless.
	for i := 0; i < 5; i++ {
		if frame := readFrame(t, source); !isSilence(frame) {
			t.Errorf("frame %d from a silent operator was not silence", i)
		}
	}

	// And it must still be transmitting after the operator leaves.
	detach()
	if frame := readFrame(t, source); !isSilence(frame) {
		t.Error("the call stopped transmitting silence after the operator detached")
	}
}

// A detaching operator takes their queued audio with them: the call keeps
// transmitting, but the next operator to attach must not open with the
// previous one's leftovers.
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

// The operator's audio still has to actually reach the peer -- a source that
// only ever returned silence would pass the test above.
func TestOperatorAudioIsTransmittedThenGivesWayToSilence(t *testing.T) {
	m, _, source := trackedCall(t)

	stream, detach, ok := m.AttachStream("chan-a", "C1")
	if !ok {
		t.Fatal("AttachStream returned false for a live call")
	}
	defer detach()

	// One full frame of s16le at a recognisable level (0x4000 == +0.5).
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

	// The operator goes quiet mid-call -- a stalled uplink, a GC pause. The
	// call must fall back to silence at once rather than freezing until the
	// next frame arrives.
	if next := readFrame(t, source); !isSilence(next) {
		t.Error("the frame after the operator went quiet was not silence")
	}
}

// I1: three callers used to compete for the library's single player slot, so a
// play command replaced the operator's player and orphaned their microphone for
// the rest of the call. Now there is one source and one Play, and the
// announcement is a phase of that source rather than a competing player.
func TestAnnouncementTakesTheFloorAndGivesItBack(t *testing.T) {
	m, lc, source := trackedCall(t)

	stream, detach, ok := m.AttachStream("chan-a", "C1")
	if !ok {
		t.Fatal("AttachStream returned false for a live call")
	}
	defer detach()

	// A full frame of announcement at 0x2000 (+0.25), distinguishable from
	// both silence and the operator's own level below.
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

	// The operator's microphone is back the moment the announcement runs out.
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

// The mirror case, and the reason the announcement is not merely queued behind
// the operator: an attach mid-announcement used to replace the announcement's
// player and truncate it silently.
func TestAttachingMidAnnouncementDoesNotTruncateIt(t *testing.T) {
	m, _, source := trackedCall(t)

	announcement := make([]byte, meowcaller.FrameSamples*2*3) // three frames
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

// The recording's operator track is tapped from this source, so it is only
// really covered here, where the frames go out through the library's own
// AudioSource rather than through a fake. Everything the peer heard belongs on
// that track -- the operator's microphone and a play command's announcement
// alike -- and none of it may leak onto the customer's.
func TestWhatTheSourceTransmitsLandsOnTheOperatorTrack(t *testing.T) {
	m, lc, source, store := trackedRecordedCall(t)

	stream, detach, ok := m.AttachStream("chan-a", "C1")
	if !ok {
		t.Fatal("AttachStream returned false for a live call")
	}
	defer detach()

	loud := make([]byte, meowcaller.FrameSamples*2)
	for i := 0; i < len(loud); i += 2 {
		loud[i], loud[i+1] = 0x00, 0x40 // +0.5
	}
	if err := stream.WriteAudio(loud); err != nil {
		t.Fatalf("WriteAudio: %v", err)
	}
	// The tap fires on the read, not on the write: what was queued but never
	// transmitted was never heard, so it is not on the record either.
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

	// The customer said nothing, and the operator's audio must not have bled
	// into their track: the split is the whole point of the three files.
	peer, ok := store.object("calls/chan-a/C1-peer.wav")
	if !ok {
		t.Fatal("the peer track was never uploaded")
	}
	if data := peer[44:]; !bytes.Equal(data, make([]byte, len(data))) {
		t.Error("the operator's audio bled into the customer's track")
	}
}

// The source transmits silence continuously for the whole life of every call,
// by design -- WhatsApp's relay will not bridge the peer back otherwise. Those
// frames must not count as audio: a call nobody spoke a word on has to leave
// the bucket exactly as empty as it was.
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

// A call that has ended must let the library's player go idle rather than
// transmitting forever. EOF is the only signal that does that.
func TestOutboundAudioEndsWhenTheCallDoes(t *testing.T) {
	_, lc, source := trackedCall(t)

	lc.fireEnd("hangup")

	if _, err := source.ReadFrame(); err != io.EOF {
		t.Errorf("ReadFrame after the call ended = %v, want io.EOF", err)
	}
}

// An announcement runs out at the call's own cadence, not at wire speed: one
// frame per read, however fast the reader asks.
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
