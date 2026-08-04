package call_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/w3nder/whatsmeow-gateway/internal/call"
)

type memPublisher struct {
	mu      sync.Mutex
	events  []call.Event
	inbound []call.InboundCallEvent
	// order records "inbound" or a call.Event's type in the sequence both
	// PublishInbound and PublishCall are actually invoked, since events and
	// inbound live in separate slices and neither alone proves which method
	// fired first.
	order []string
}

func (p *memPublisher) PublishCall(_ context.Context, evt call.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, evt)
	p.order = append(p.order, evt.Type)
	return nil
}

func (p *memPublisher) PublishInbound(_ context.Context, evt call.InboundCallEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inbound = append(p.inbound, evt)
	p.order = append(p.order, "inbound")
	return nil
}

func (p *memPublisher) inboundEvents() []call.InboundCallEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]call.InboundCallEvent(nil), p.inbound...)
}

// sequence returns the order publish calls actually landed in, "inbound" or a
// call.Event's type per entry.
func (p *memPublisher) sequence() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.order...)
}

func (p *memPublisher) typed(t string) []call.Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []call.Event
	for _, e := range p.events {
		if e.Type == t {
			out = append(out, e)
		}
	}
	return out
}

type panicPublisher struct{}

func (panicPublisher) PublishCall(context.Context, call.Event) error { panic("boom") }
func (panicPublisher) PublishInbound(context.Context, call.InboundCallEvent) error {
	panic("boom")
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// newTestManager's identity function derives PhoneNumberID from the channel
// id, mirroring gateway.callIdentity in production: the backend finds a
// gateway channel by that field holding the channel's own id, not a phone
// number. Deriving it here means a test that asserts PhoneNumberID against
// the channel id it attached is pinned to that relationship, not to a
// fixture constant that would mask a regression back to the device JID.
func newTestManager(t *testing.T, pub call.Publisher, store call.RecordingStore, now func() time.Time) *call.Manager {
	t.Helper()
	return call.NewManager(pub, store,
		func(channelID string) call.Identity {
			return call.Identity{PhoneNumberID: channelID, TenantID: "t1"}
		},
		nil,
		call.Options{TmpDir: t.TempDir(), Record: true, Now: now},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
}

func TestManagerPublishesIncomingCall(t *testing.T) {
	pub := &memPublisher{}
	m := newTestManager(t, pub, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	caller.fireIncoming(&fakeCall{id: "C1", peer: "5511888888888@s.whatsapp.net", video: true})

	incoming := pub.typed(call.EventIncoming)
	if len(incoming) != 1 {
		t.Fatalf("got %d incoming events, want 1", len(incoming))
	}
	evt := incoming[0]
	if evt.CallID != "C1" {
		t.Errorf("CallID = %q, want C1", evt.CallID)
	}
	if evt.Direction != call.DirectionInbound {
		t.Errorf("Direction = %q, want inbound", evt.Direction)
	}
	if !evt.IsVideo {
		t.Error("IsVideo = false, want true for a video offer")
	}
	// From is the resolved phone-number user, not the raw peer JID string --
	// the same shape an inbound message's From takes, since both derive from
	// senderid.Resolve/From on the same JID.
	if evt.From != "5511888888888" {
		t.Errorf("From = %q, want the resolved phone number", evt.From)
	}
	if evt.TenantID != "t1" || evt.PhoneNumberID != "chan-a" {
		t.Errorf("identity = %q/%q, want t1/chan-a", evt.TenantID, evt.PhoneNumberID)
	}
	if _, ok := m.Get("chan-a", "C1"); !ok {
		t.Error("incoming call was not registered")
	}
}

// PhoneNumberID must track the channel id on both the call.Event and the
// inbound-message-shaped call event: the backend resolves a channel by that
// field holding the channel's own id, not a phone number or device JID. Two
// different channel ids are attached here so the assertion actually pins the
// relationship to whatever channel produced the event -- a fixture that
// returned one fixed PhoneNumberID regardless of channel would pass a
// same-channel-only check without proving the mapping is right.
func TestPhoneNumberIDMatchesChannelID(t *testing.T) {
	pub := &memPublisher{}
	m := newTestManager(t, pub, newMemStore(), time.Now)

	callerX := &fakeCaller{}
	m.Attach("chan-x", callerX)
	callerX.fireIncoming(&fakeCall{id: "CX", peer: "5511888888888@s.whatsapp.net"})

	callerY := &fakeCaller{}
	m.Attach("chan-y", callerY)
	callerY.fireIncoming(&fakeCall{id: "CY", peer: "5511888888888@s.whatsapp.net"})

	events := pub.typed(call.EventIncoming)
	if len(events) != 2 {
		t.Fatalf("got %d incoming events, want 2", len(events))
	}
	for _, evt := range events {
		want := "chan-x"
		if evt.CallID == "CY" {
			want = "chan-y"
		}
		if evt.PhoneNumberID != want {
			t.Errorf("call %s: PhoneNumberID = %q, want %q (its own channel id)", evt.CallID, evt.PhoneNumberID, want)
		}
	}

	inbound := pub.inboundEvents()
	if len(inbound) != 2 {
		t.Fatalf("got %d inbound events, want 2", len(inbound))
	}
	for _, evt := range inbound {
		want := "chan-x"
		if evt.ProviderMessageID == "CY" {
			want = "chan-y"
		}
		if evt.PhoneNumberID != want {
			t.Errorf("call %s: InboundCallEvent.PhoneNumberID = %q, want %q (its own channel id)", evt.ProviderMessageID, evt.PhoneNumberID, want)
		}
	}
}

// Talk time is measured from the answer, not from the offer.
func TestManagerEndedCarriesAnsweredDuration(t *testing.T) {
	pub := &memPublisher{}
	clock := &fakeClock{now: time.Unix(1_754_300_000, 0)}
	m := newTestManager(t, pub, newMemStore(), clock.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	lc := &fakeCall{id: "C1", peer: "5511888888888@s.whatsapp.net"}
	caller.fireIncoming(lc)

	clock.advance(40 * time.Second) // ringing
	lc.fireReady()
	clock.advance(10 * time.Second) // talking
	lc.fireEnd("hangup")

	ended := pub.typed(call.EventEnded)
	if len(ended) != 1 {
		t.Fatalf("got %d ended events, want 1", len(ended))
	}
	if ended[0].Duration != 10 {
		t.Errorf("Duration = %d, want 10 (answered to end, not offer to end)", ended[0].Duration)
	}
	if ended[0].Reason != "hangup" {
		t.Errorf("Reason = %q, want hangup", ended[0].Reason)
	}
	if _, ok := m.Get("chan-a", "C1"); ok {
		t.Error("call still registered after ending")
	}
}

// A call that was never answered ends with zero duration, not with the ring
// time.
func TestManagerUnansweredCallHasZeroDuration(t *testing.T) {
	pub := &memPublisher{}
	clock := &fakeClock{now: time.Unix(1_754_300_000, 0)}
	m := newTestManager(t, pub, newMemStore(), clock.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	lc := &fakeCall{id: "C1"}
	caller.fireIncoming(lc)
	clock.advance(45 * time.Second)
	lc.fireEnd("timeout")

	ended := pub.typed(call.EventEnded)
	if len(ended) != 1 || ended[0].Duration != 0 {
		t.Fatalf("ended = %+v, want one event with duration 0", ended)
	}
}

// The recording is a by-product of the call, not a gate on it: the ended
// event must never carry media, and the recording arrives later as its own
// event once the upload finishes.
func TestManagerEndedCarriesRecording(t *testing.T) {
	pub := &memPublisher{}
	store := newMemStore()
	m := newTestManager(t, pub, store, time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	lc := &fakeCall{id: "C1", peer: "5511888888888@s.whatsapp.net"}
	caller.fireIncoming(lc)
	lc.fireReady()
	lc.feedAudio([]float32{0.5, -0.5})
	lc.fireEnd("hangup")
	m.WaitForRecordings(2 * time.Second)

	ended := pub.typed(call.EventEnded)
	if len(ended) != 1 {
		t.Fatalf("got %d ended events, want 1", len(ended))
	}
	if ended[0].Media != nil {
		t.Errorf("ended Media = %+v, want nil: the recording is a separate event", ended[0].Media)
	}

	recording := pub.typed(call.EventRecording)
	if len(recording) != 1 {
		t.Fatalf("got %d recording events, want 1", len(recording))
	}
	if recording[0].Media == nil || recording[0].Media.Key != "calls/chan-a/C1.wav" {
		t.Fatalf("Media = %+v, want the wav recording key", recording[0].Media)
	}
	if _, ok := store.object("calls/chan-a/C1.wav"); !ok {
		t.Error("recording was never uploaded")
	}
}

func TestManagerEndedCarriesVideoRecording(t *testing.T) {
	pub := &memPublisher{}
	store := newMemStore()
	m := newTestManager(t, pub, store, time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	lc := &fakeCall{id: "C1", video: true}
	caller.fireIncoming(lc)
	lc.fireReady()
	lc.feedAudio([]float32{0.5})
	lc.feedVideo([]byte{0, 0, 0, 1, 0x65, 0xAA})
	lc.fireEnd("hangup")
	m.WaitForRecordings(2 * time.Second)

	recording := pub.typed(call.EventRecording)[0]
	if recording.VideoMedia == nil || recording.VideoMedia.Key != "calls/chan-a/C1.h264" {
		t.Fatalf("VideoMedia = %+v, want the h264 recording key", recording.VideoMedia)
	}
	if _, ok := store.object("calls/chan-a/C1.h264"); !ok {
		t.Error("video recording was never uploaded")
	}
}

// Losing the recording must not hide the end of the call from the backend.
func TestManagerPublishesEndedEvenWhenUploadFails(t *testing.T) {
	pub := &memPublisher{}
	store := newMemStore()
	store.fail(errors.New("bucket is on fire"))
	m := newTestManager(t, pub, store, time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	lc := &fakeCall{id: "C1"}
	caller.fireIncoming(lc)
	lc.fireReady()
	lc.feedAudio([]float32{0.5})
	lc.fireEnd("hangup")
	m.WaitForRecordings(2 * time.Second)

	ended := pub.typed(call.EventEnded)
	if len(ended) != 1 {
		t.Fatalf("got %d ended events, want 1", len(ended))
	}
	if ended[0].Media != nil {
		t.Errorf("Media = %+v, want nil when the upload failed", ended[0].Media)
	}
	if recording := pub.typed(call.EventRecording); len(recording) != 0 {
		t.Errorf("recording events = %+v, want none: nothing uploaded successfully", recording)
	}
	failed := pub.typed(call.EventCommandFailed)
	if len(failed) != 1 || failed[0].Error == nil || failed[0].Error.Code != call.CodeRecordingUploadFailed {
		t.Errorf("failures = %+v, want one recording_upload_failed", failed)
	}
}

// A slow object store must never delay the event that tells the backend the
// call is over: ended goes out synchronously, before the upload even has a
// chance to finish.
func TestManagerPublishesEndedBeforeRecordingUploadCompletes(t *testing.T) {
	pub := &memPublisher{}
	store := newMemStore()
	release := store.blockPuts()
	t.Cleanup(release)
	m := newTestManager(t, pub, store, time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	lc := &fakeCall{id: "C1"}
	caller.fireIncoming(lc)
	lc.fireReady()
	lc.feedAudio([]float32{0.5})
	lc.fireEnd("hangup")

	// The upload is still blocked on the store: ended must already be out,
	// and the recording must not be, proving the ordering rather than timing
	// it.
	if ended := pub.typed(call.EventEnded); len(ended) != 1 {
		t.Fatalf("got %d ended events before the upload unblocked, want 1", len(ended))
	}
	if recording := pub.typed(call.EventRecording); len(recording) != 0 {
		t.Fatalf("recording events = %+v, want none before the upload unblocked", recording)
	}

	release()
	m.WaitForRecordings(2 * time.Second)

	recordingEvents := pub.typed(call.EventRecording)
	if len(recordingEvents) != 1 || recordingEvents[0].Media == nil {
		t.Fatalf("recording events = %+v, want one carrying media", recordingEvents)
	}

	// Confirm the relative order in the raw event stream: ended must precede
	// recording, never the reverse.
	endedIdx, recordingIdx := -1, -1
	for i, e := range pub.events {
		switch e.Type {
		case call.EventEnded:
			endedIdx = i
		case call.EventRecording:
			recordingIdx = i
		}
	}
	if endedIdx == -1 || recordingIdx == -1 || recordingIdx < endedIdx {
		t.Fatalf("event order wrong: ended at %d, recording at %d", endedIdx, recordingIdx)
	}
}

// The recording upload must survive shutdown: WaitForRecordings has to
// actually wait for an in-flight upload rather than returning immediately.
func TestManagerWaitForRecordingsWaitsForInFlightUpload(t *testing.T) {
	pub := &memPublisher{}
	store := newMemStore()
	release := store.blockPuts()
	m := newTestManager(t, pub, store, time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	lc := &fakeCall{id: "C1"}
	caller.fireIncoming(lc)
	lc.fireReady()
	lc.feedAudio([]float32{0.5})
	lc.fireEnd("hangup")

	waitDone := make(chan struct{})
	go func() {
		m.WaitForRecordings(5 * time.Second)
		close(waitDone)
	}()

	select {
	case <-waitDone:
		t.Fatal("WaitForRecordings returned before the upload finished")
	case <-time.After(200 * time.Millisecond):
		// Still blocked, as expected.
	}

	release()

	select {
	case <-waitDone:
	case <-time.After(2 * time.Second):
		t.Fatal("WaitForRecordings did not return after the upload finished")
	}

	if recording := pub.typed(call.EventRecording); len(recording) != 1 {
		t.Errorf("recording events = %+v, want one", recording)
	}
}

// WaitForRecordings must not hang forever on an upload that never finishes;
// it is a bounded wait, not a guarantee.
func TestManagerWaitForRecordingsRespectsDeadline(t *testing.T) {
	pub := &memPublisher{}
	store := newMemStore()
	release := store.blockPuts()
	t.Cleanup(release)
	m := newTestManager(t, pub, store, time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	lc := &fakeCall{id: "C1"}
	caller.fireIncoming(lc)
	lc.fireReady()
	lc.feedAudio([]float32{0.5})
	lc.fireEnd("hangup")

	start := time.Now()
	m.WaitForRecordings(100 * time.Millisecond)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("WaitForRecordings took %s, want it bounded near its 100ms deadline", elapsed)
	}
}

func TestManagerRecordingCanBeDisabled(t *testing.T) {
	pub := &memPublisher{}
	store := newMemStore()
	m := call.NewManager(pub, store,
		func(string) call.Identity { return call.Identity{TenantID: "t1"} },
		nil,
		call.Options{TmpDir: t.TempDir(), Record: false, Now: time.Now},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	lc := &fakeCall{id: "C1"}
	caller.fireIncoming(lc)
	lc.fireReady()
	lc.feedAudio([]float32{0.5})
	lc.fireEnd("hangup")

	if store.objectCount() != 0 {
		t.Errorf("uploaded %d objects with recording off, want none", store.objectCount())
	}
	if ended := pub.typed(call.EventEnded); len(ended) != 1 || ended[0].Media != nil {
		t.Errorf("ended = %+v, want one event with no media", ended)
	}
}

func TestManagerAbortChannelEndsLiveCalls(t *testing.T) {
	pub := &memPublisher{}
	m := newTestManager(t, pub, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	lc := &fakeCall{id: "C1"}
	caller.fireIncoming(lc)
	lc.fireReady()

	m.AbortChannel(context.Background(), "chan-a", "disconnected")

	ended := pub.typed(call.EventEnded)
	if len(ended) != 1 || ended[0].Reason != "disconnected" {
		t.Fatalf("ended = %+v, want one ended with reason disconnected", ended)
	}
	if _, ok := m.Get("chan-a", "C1"); ok {
		t.Error("call still registered after AbortChannel")
	}
}

// A call must never end twice, whichever path gets there first.
func TestManagerEndIsIdempotent(t *testing.T) {
	pub := &memPublisher{}
	m := newTestManager(t, pub, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	lc := &fakeCall{id: "C1"}
	caller.fireIncoming(lc)
	lc.fireReady()
	lc.fireEnd("hangup")
	lc.fireEnd("hangup")
	m.AbortChannel(context.Background(), "chan-a", "disconnected")

	if ended := pub.typed(call.EventEnded); len(ended) != 1 {
		t.Fatalf("got %d ended events, want exactly 1", len(ended))
	}
}

func TestManagerPublishesVideoStateAndReaction(t *testing.T) {
	pub := &memPublisher{}
	m := newTestManager(t, pub, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	lc := &fakeCall{id: "C1"}
	caller.fireIncoming(lc)

	lc.fireVideoState(call.VideoState{Active: true, Upgrade: true, Orientation: 2, Raw: 11})
	lc.fireReaction(call.Reaction{Emoji: "👍", Sender: "5511888888888@s.whatsapp.net"})
	lc.fireGroupState(call.GroupState{
		TransactionID: 7,
		Participants:  []call.GroupParticipant{{JID: "a@s.whatsapp.net", State: "connected"}},
	})

	video := pub.typed(call.EventVideo)
	if len(video) != 1 || video[0].Video == nil || !video[0].Video.Upgrade || video[0].Video.Orientation != 2 {
		t.Errorf("video events = %+v, want one upgrade at orientation 2", video)
	}
	reaction := pub.typed(call.EventReactionType)
	if len(reaction) != 1 || reaction[0].Reaction == nil || reaction[0].Reaction.Emoji != "👍" {
		t.Errorf("reaction events = %+v, want one thumbs up", reaction)
	}
	group := pub.typed(call.EventGroupState)
	if len(group) != 1 || group[0].Group == nil || len(group[0].Group.Participants) != 1 {
		t.Errorf("group events = %+v, want one roster with a participant", group)
	}
}

// A client built without a calling stack must not take the channel down.
func TestManagerAttachToleratesNilCaller(t *testing.T) {
	m := newTestManager(t, &memPublisher{}, newMemStore(), time.Now)

	// Must not panic.
	m.Attach("chan-a", nil)
}

// A callback runs on the library's media goroutine. A panic there must not take
// the gateway down with it.
func TestManagerSurvivesPanickingPublish(t *testing.T) {
	m := call.NewManager(panicPublisher{}, newMemStore(),
		func(string) call.Identity { return call.Identity{} },
		nil,
		call.Options{TmpDir: t.TempDir(), Record: true, Now: time.Now},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	// Must not panic.
	caller.fireIncoming(&fakeCall{id: "C1"})
}
