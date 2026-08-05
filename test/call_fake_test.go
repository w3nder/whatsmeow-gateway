package test

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/w3nder/whatsmeow-gateway/internal/call"
)

// The call fakes in internal/call live in _test.go files, which Go cannot
// import, so the end-to-end test carries its own -- the same arrangement as
// fakeWAClient.

// fakeCaller stands in for a channel's calling client, exposing the
// incoming-call handler the gateway registers so the test can fire a call.
type fakeCaller struct {
	mu       sync.Mutex
	incoming func(call.LiveCall)
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
	return &fakeLiveCall{callID: "OUT1", peer: target, video: video}, nil
}

func (f *fakeCaller) GroupCall(context.Context, []string, call.GroupOptions) (call.LiveCall, error) {
	return &fakeLiveCall{callID: "GRP1"}, nil
}

func (f *fakeCaller) GroupCallByID(context.Context, string, call.GroupOptions) (call.LiveCall, error) {
	return &fakeLiveCall{callID: "GRP2"}, nil
}

func (f *fakeCaller) CreateCallLink(context.Context, bool) (call.Link, error) {
	return call.Link{Token: "TOK"}, nil
}

func (f *fakeCaller) PreviewCallLink(context.Context, string, bool) (call.LinkPreview, error) {
	return call.LinkPreview{Token: "TOK"}, nil
}

func (f *fakeCaller) JoinCallLink(context.Context, string, bool) (call.LiveCall, error) {
	return &fakeLiveCall{callID: "LINK1"}, nil
}

var _ call.Caller = (*fakeCaller)(nil)

// fakeLiveCall records what the gateway does to a call and lets the test drive
// the call's lifecycle and its inbound audio.
type fakeLiveCall struct {
	callID string
	peer   string
	video  bool

	mu                sync.Mutex
	actions           []string
	played            []io.ReadCloser
	onReady           func()
	onEnd             func(string)
	audioIn           func([]float32)
	videoIn           func([]byte)
	onKeyframeRequest func()
}

func (f *fakeLiveCall) record(action string) error {
	f.mu.Lock()
	f.actions = append(f.actions, action)
	f.mu.Unlock()
	return nil
}

func (f *fakeLiveCall) recordedActions() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.actions...)
}

func (f *fakeLiveCall) fireReady() {
	f.mu.Lock()
	fn := f.onReady
	f.mu.Unlock()
	if fn != nil {
		fn()
	}
}

func (f *fakeLiveCall) fireEnd(reason string) {
	f.mu.Lock()
	fn := f.onEnd
	f.mu.Unlock()
	if fn != nil {
		fn(reason)
	}
}

// playedSrc returns the reader from the most recent Play call, or nil if Play
// was never called. Lets a test read back what the operator actually wrote,
// rather than only observing that Play was wired up.
func (f *fakeLiveCall) playedSrc() io.ReadCloser {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.played) == 0 {
		return nil
	}
	return f.played[len(f.played)-1]
}

func (f *fakeLiveCall) feedAudio(frame []float32) {
	f.mu.Lock()
	fn := f.audioIn
	f.mu.Unlock()
	if fn != nil {
		fn(frame)
	}
}

func (f *fakeLiveCall) ID() string    { return f.callID }
func (f *fakeLiveCall) Peer() string  { return f.peer }
func (f *fakeLiveCall) IsVideo() bool { return f.video }

func (f *fakeLiveCall) Answer() error                         { return f.record("answer") }
func (f *fakeLiveCall) Reject() error                         { return f.record("reject") }
func (f *fakeLiveCall) Hangup() error                         { return f.record("hangup") }
func (f *fakeLiveCall) StartVideo() error                     { return f.record("video.start") }
func (f *fakeLiveCall) AcceptVideo() error                    { return f.record("video.accept") }
func (f *fakeLiveCall) StopVideo() error                      { return f.record("video.stop") }
func (f *fakeLiveCall) SetVideoEnabled(bool) error            { return f.record("video.enable") }
func (f *fakeLiveCall) SetVideoOrientation(int) error         { return f.record("video.orientation") }
func (f *fakeLiveCall) SendVideo([]byte, time.Duration) error { return f.record("video.send") }
func (f *fakeLiveCall) SendReaction(string) error             { return f.record("reaction") }
func (f *fakeLiveCall) SetHandRaised(bool) error              { return f.record("hand.raise") }
func (f *fakeLiveCall) StartScreenShare(*uint32) error        { return f.record("screenshare.start") }
func (f *fakeLiveCall) StopScreenShare() error                { return f.record("screenshare.stop") }

// Play records the source it was handed, and deliberately not an action: the
// gateway subscribes a call's outbound audio at Track time, so counting it as
// an action would show up in every assertion about what a command did.
func (f *fakeLiveCall) Play(src io.ReadCloser) error {
	f.mu.Lock()
	f.played = append(f.played, src)
	f.mu.Unlock()
	return nil
}

func (f *fakeLiveCall) AddParticipant(context.Context, string) error {
	return f.record("participant.add")
}

func (f *fakeLiveCall) RingParticipant(context.Context, string) error {
	return f.record("participant.ring")
}

func (f *fakeLiveCall) SetApprovalRequired(context.Context, bool) error {
	return f.record("approval.set")
}

func (f *fakeLiveCall) AdmitParticipant(context.Context, string) error {
	return f.record("participant.admit")
}

func (f *fakeLiveCall) DenyParticipant(context.Context, string) error {
	return f.record("participant.deny")
}

func (f *fakeLiveCall) Receive(sink func([]float32)) {
	f.mu.Lock()
	f.audioIn = sink
	f.mu.Unlock()
}

func (f *fakeLiveCall) ReceiveVideo(sink func([]byte)) {
	f.mu.Lock()
	f.videoIn = sink
	f.mu.Unlock()
}

func (f *fakeLiveCall) OnReady(fn func()) {
	f.mu.Lock()
	f.onReady = fn
	f.mu.Unlock()
}

func (f *fakeLiveCall) OnEnd(fn func(string)) {
	f.mu.Lock()
	f.onEnd = fn
	f.mu.Unlock()
}

func (f *fakeLiveCall) OnStateChange(func(call.Phase))            {}
func (f *fakeLiveCall) OnPeerAccept(func())                       {}
func (f *fakeLiveCall) OnMuteState(func(bool))                    {}
func (f *fakeLiveCall) OnVideoState(func(call.VideoState))        {}
func (f *fakeLiveCall) OnReaction(func(call.Reaction))            {}
func (f *fakeLiveCall) OnGroupState(func(call.GroupState))        {}
func (f *fakeLiveCall) OnWaitingRoomState(func(call.WaitingRoom)) {}
func (f *fakeLiveCall) OnHandRaise(func(call.HandState))          {}
func (f *fakeLiveCall) OnScreenShare(func(call.ScreenShare))      {}

func (f *fakeLiveCall) OnVideoKeyframeRequest(fn func()) {
	f.mu.Lock()
	f.onKeyframeRequest = fn
	f.mu.Unlock()
}

// fireVideoKeyframeRequest stands in for WhatsApp's PLI/FIR feedback asking
// our outgoing video for a fresh IDR.
func (f *fakeLiveCall) fireVideoKeyframeRequest() {
	f.mu.Lock()
	fn := f.onKeyframeRequest
	f.mu.Unlock()
	if fn != nil {
		fn()
	}
}

var _ call.LiveCall = (*fakeLiveCall)(nil)
