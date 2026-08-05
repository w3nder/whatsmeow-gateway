package call_test

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/w3nder/whatsmeow-gateway/internal/call"
)

// fakeCall stands in for a live call: it records the actions taken on it and
// lets a test drive the callbacks the library would otherwise fire from its
// media loop.
type fakeCall struct {
	id    string
	peer  string
	video bool

	mu       sync.Mutex
	actions  []string
	videoOut [][]byte
	played   []io.ReadCloser
	lastArg  string

	onReady           func()
	onEnd             func(string)
	onState           func(call.Phase)
	onPeerAccept      func()
	onMute            func(bool)
	onVideoState      func(call.VideoState)
	onReaction        func(call.Reaction)
	onGroupState      func(call.GroupState)
	onWaitingRoom     func(call.WaitingRoom)
	onHand            func(call.HandState)
	onScreenShare     func(call.ScreenShare)
	onKeyframeRequest func()

	audioIn func([]float32)
	videoIn func([]byte)
}

func (f *fakeCall) record(action string) error {
	f.mu.Lock()
	f.actions = append(f.actions, action)
	f.mu.Unlock()
	return nil
}

func (f *fakeCall) recordArg(action, arg string) error {
	f.mu.Lock()
	f.actions = append(f.actions, action)
	f.lastArg = arg
	f.mu.Unlock()
	return nil
}

func (f *fakeCall) recordedActions() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.actions...)
}

func (f *fakeCall) recordedArg() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastArg
}

func (f *fakeCall) sentVideo() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]byte(nil), f.videoOut...)
}

// playedSrc returns the reader from the most recent Play call, or nil if Play
// was never called. It is the seam every outbound-audio test reads through:
// asserting that Play happened proves nothing, since Manager.Track subscribes
// the call's source before anyone has written a byte into it.
func (f *fakeCall) playedSrc() io.ReadCloser {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.played) == 0 {
		return nil
	}
	return f.played[len(f.played)-1]
}

func (f *fakeCall) ID() string    { return f.id }
func (f *fakeCall) Peer() string  { return f.peer }
func (f *fakeCall) IsVideo() bool { return f.video }

func (f *fakeCall) Answer() error                  { return f.record("answer") }
func (f *fakeCall) Reject() error                  { return f.record("reject") }
func (f *fakeCall) Hangup() error                  { return f.record("hangup") }
func (f *fakeCall) StartVideo() error              { return f.record("video.start") }
func (f *fakeCall) AcceptVideo() error             { return f.record("video.accept") }
func (f *fakeCall) StopVideo() error               { return f.record("video.stop") }
func (f *fakeCall) SetVideoEnabled(bool) error     { return f.record("video.enable") }
func (f *fakeCall) SetVideoOrientation(int) error  { return f.record("video.orientation") }
func (f *fakeCall) SendReaction(e string) error    { return f.recordArg("reaction", e) }
func (f *fakeCall) SetHandRaised(bool) error       { return f.record("hand.raise") }
func (f *fakeCall) StartScreenShare(*uint32) error { return f.record("screenshare.start") }
func (f *fakeCall) StopScreenShare() error         { return f.record("screenshare.stop") }

func (f *fakeCall) AddParticipant(_ context.Context, t string) error {
	return f.recordArg("participant.add", t)
}
func (f *fakeCall) RingParticipant(_ context.Context, t string) error {
	return f.recordArg("participant.ring", t)
}
func (f *fakeCall) SetApprovalRequired(context.Context, bool) error {
	return f.record("approval.set")
}
func (f *fakeCall) AdmitParticipant(_ context.Context, u string) error {
	return f.recordArg("participant.admit", u)
}
func (f *fakeCall) DenyParticipant(_ context.Context, u string) error {
	return f.recordArg("participant.deny", u)
}

func (f *fakeCall) SendVideo(au []byte, _ time.Duration) error {
	f.mu.Lock()
	f.videoOut = append(f.videoOut, au)
	f.mu.Unlock()
	return nil
}

// Play records the source it was handed. It deliberately does not record an
// action: Play is call setup now, not a command, so counting it among the
// actions would make every recordedActions assertion in the suite fire on
// something no command asked for.
func (f *fakeCall) Play(src io.ReadCloser) error {
	f.mu.Lock()
	f.played = append(f.played, src)
	f.mu.Unlock()
	return nil
}

// playCount reports how many times Play was called. One per call is the
// invariant: a second subscribe silently orphans the first player.
func (f *fakeCall) playCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.played)
}

func (f *fakeCall) Receive(sink func([]float32)) {
	f.mu.Lock()
	f.audioIn = sink
	f.mu.Unlock()
}

func (f *fakeCall) ReceiveVideo(sink func([]byte)) {
	f.mu.Lock()
	f.videoIn = sink
	f.mu.Unlock()
}

func (f *fakeCall) OnReady(fn func()) {
	f.mu.Lock()
	f.onReady = fn
	f.mu.Unlock()
}

func (f *fakeCall) OnEnd(fn func(string)) {
	f.mu.Lock()
	f.onEnd = fn
	f.mu.Unlock()
}

func (f *fakeCall) OnStateChange(fn func(call.Phase)) {
	f.mu.Lock()
	f.onState = fn
	f.mu.Unlock()
}

func (f *fakeCall) OnPeerAccept(fn func()) {
	f.mu.Lock()
	f.onPeerAccept = fn
	f.mu.Unlock()
}

func (f *fakeCall) OnMuteState(fn func(bool)) {
	f.mu.Lock()
	f.onMute = fn
	f.mu.Unlock()
}

func (f *fakeCall) OnVideoState(fn func(call.VideoState)) {
	f.mu.Lock()
	f.onVideoState = fn
	f.mu.Unlock()
}

func (f *fakeCall) OnReaction(fn func(call.Reaction)) {
	f.mu.Lock()
	f.onReaction = fn
	f.mu.Unlock()
}

func (f *fakeCall) OnGroupState(fn func(call.GroupState)) {
	f.mu.Lock()
	f.onGroupState = fn
	f.mu.Unlock()
}

func (f *fakeCall) OnWaitingRoomState(fn func(call.WaitingRoom)) {
	f.mu.Lock()
	f.onWaitingRoom = fn
	f.mu.Unlock()
}

func (f *fakeCall) OnHandRaise(fn func(call.HandState)) {
	f.mu.Lock()
	f.onHand = fn
	f.mu.Unlock()
}

func (f *fakeCall) OnScreenShare(fn func(call.ScreenShare)) {
	f.mu.Lock()
	f.onScreenShare = fn
	f.mu.Unlock()
}

func (f *fakeCall) OnVideoKeyframeRequest(fn func()) {
	f.mu.Lock()
	f.onKeyframeRequest = fn
	f.mu.Unlock()
}

// The fire* helpers stand in for the library's media loop firing a callback.

func (f *fakeCall) fireReady() {
	f.mu.Lock()
	fn := f.onReady
	f.mu.Unlock()
	if fn != nil {
		fn()
	}
}

func (f *fakeCall) fireEnd(reason string) {
	f.mu.Lock()
	fn := f.onEnd
	f.mu.Unlock()
	if fn != nil {
		fn(reason)
	}
}

func (f *fakeCall) fireVideoState(v call.VideoState) {
	f.mu.Lock()
	fn := f.onVideoState
	f.mu.Unlock()
	if fn != nil {
		fn(v)
	}
}

func (f *fakeCall) fireReaction(r call.Reaction) {
	f.mu.Lock()
	fn := f.onReaction
	f.mu.Unlock()
	if fn != nil {
		fn(r)
	}
}

func (f *fakeCall) fireGroupState(g call.GroupState) {
	f.mu.Lock()
	fn := f.onGroupState
	f.mu.Unlock()
	if fn != nil {
		fn(g)
	}
}

func (f *fakeCall) feedAudio(frame []float32) {
	f.mu.Lock()
	fn := f.audioIn
	f.mu.Unlock()
	if fn != nil {
		fn(frame)
	}
}

func (f *fakeCall) fireVideoKeyframeRequest() {
	f.mu.Lock()
	fn := f.onKeyframeRequest
	f.mu.Unlock()
	if fn != nil {
		fn()
	}
}

func (f *fakeCall) feedVideo(accessUnit []byte) {
	f.mu.Lock()
	fn := f.videoIn
	f.mu.Unlock()
	if fn != nil {
		fn(accessUnit)
	}
}

var _ call.LiveCall = (*fakeCall)(nil)

// fakeCaller stands in for the calling client of one channel.
type fakeCaller struct {
	mu       sync.Mutex
	incoming func(call.LiveCall)
	placed   *fakeCall
	callErr  error

	lastTargets []string
	lastGroupID string
	lastOpts    call.GroupOptions
}

func (f *fakeCaller) OnIncomingCall(fn func(call.LiveCall)) {
	f.mu.Lock()
	f.incoming = fn
	f.mu.Unlock()
}

func (f *fakeCaller) incomingHandler() func(call.LiveCall) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.incoming
}

func (f *fakeCaller) fireIncoming(lc call.LiveCall) {
	if fn := f.incomingHandler(); fn != nil {
		fn(lc)
	}
}

func (f *fakeCaller) Call(_ context.Context, target string, video bool) (call.LiveCall, error) {
	if f.callErr != nil {
		return nil, f.callErr
	}
	f.mu.Lock()
	f.placed = &fakeCall{id: "OUT1", peer: target, video: video}
	placed := f.placed
	f.mu.Unlock()
	return placed, nil
}

func (f *fakeCaller) GroupCall(_ context.Context, targets []string, opts call.GroupOptions) (call.LiveCall, error) {
	if f.callErr != nil {
		return nil, f.callErr
	}
	f.mu.Lock()
	f.lastTargets = targets
	f.lastOpts = opts
	f.placed = &fakeCall{id: "GRP1", video: opts.Video}
	placed := f.placed
	f.mu.Unlock()
	return placed, nil
}

func (f *fakeCaller) GroupCallByID(_ context.Context, groupID string, opts call.GroupOptions) (call.LiveCall, error) {
	if f.callErr != nil {
		return nil, f.callErr
	}
	f.mu.Lock()
	f.lastGroupID = groupID
	f.lastOpts = opts
	f.placed = &fakeCall{id: "GRP2", video: opts.Video}
	placed := f.placed
	f.mu.Unlock()
	return placed, nil
}

func (f *fakeCaller) CreateCallLink(_ context.Context, video bool) (call.Link, error) {
	return call.Link{Token: "TOK", URL: "https://call.whatsapp.com/voice/TOK", Video: video}, nil
}

func (f *fakeCaller) PreviewCallLink(context.Context, string, bool) (call.LinkPreview, error) {
	return call.LinkPreview{Token: "TOK", ApprovalRequired: true}, nil
}

func (f *fakeCaller) JoinCallLink(context.Context, string, bool) (call.LiveCall, error) {
	f.mu.Lock()
	f.placed = &fakeCall{id: "LINK1"}
	placed := f.placed
	f.mu.Unlock()
	return placed, nil
}

var _ call.Caller = (*fakeCaller)(nil)

func TestFakesSatisfyThePort(t *testing.T) {
	var lc call.LiveCall = &fakeCall{id: "ABC", peer: "5511888888888@s.whatsapp.net"}
	if lc.ID() != "ABC" {
		t.Errorf("ID() = %q, want ABC", lc.ID())
	}
	if lc.Peer() != "5511888888888@s.whatsapp.net" {
		t.Errorf("Peer() = %q, want the peer jid", lc.Peer())
	}

	var c call.Caller = &fakeCaller{}
	placed, err := c.Call(context.Background(), "+5511888888888", true)
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if !placed.IsVideo() {
		t.Error("IsVideo() = false, want true for a video call")
	}
}
