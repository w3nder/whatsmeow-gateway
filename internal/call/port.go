package call

import (
	"context"
	"io"
	"time"
)

// The interfaces here are the gateway's own, not the calling library's, and the
// indirection is deliberate. The library's Call and Client are driven by a live
// VoIP stack -- relay sockets, SRTP keys, a running media loop -- and cannot be
// constructed in a test. Everything in this package programs against these
// interfaces instead, so the whole command and event surface is exercised
// without a real call, and exactly one production file
// (internal/session/caller.go) knows the library's concrete types.
//
// One test file breaks that on purpose: outbound_test.go reads the call's
// outbound audio through the library's own AudioSource, because the one thing
// a fake cannot prove is that the source never parks the library's synchronous
// send loop.

// GroupOptions controls a group call's media and its optional chat binding.
type GroupOptions struct {
	// GroupJID binds the call to a WhatsApp group. Empty places an ad-hoc call.
	GroupJID string
	Video    bool
}

// Link is a reusable public call link.
type Link struct {
	Token string
	URL   string
	Video bool
}

// LinkPreview is a call link's metadata, read without joining.
type LinkPreview struct {
	Token            string
	Video            bool
	ApprovalRequired bool
	IsAdmin          bool
	Creator          string
	CreatorPN        string
}

// Caller places calls and receives them for one channel.
type Caller interface {
	// Call places a 1:1 call to target, which may be a phone number or a JID.
	Call(ctx context.Context, target string, video bool) (LiveCall, error)
	// GroupCall places a call to at least two remote targets.
	GroupCall(ctx context.Context, targets []string, opts GroupOptions) (LiveCall, error)
	// GroupCallByID resolves a group's roster and calls every remote member.
	GroupCallByID(ctx context.Context, groupID string, opts GroupOptions) (LiveCall, error)
	CreateCallLink(ctx context.Context, video bool) (Link, error)
	PreviewCallLink(ctx context.Context, tokenOrURL string, video bool) (LinkPreview, error)
	JoinCallLink(ctx context.Context, tokenOrURL string, video bool) (LiveCall, error)
	// OnIncomingCall registers the listener for inbound offers. The call it
	// receives has not been answered yet.
	OnIncomingCall(fn func(LiveCall))
}

// LiveCall is one call in flight.
type LiveCall interface {
	ID() string
	// Peer is the remote party, as a JID string.
	Peer() string
	IsVideo() bool

	Answer() error
	Reject() error
	Hangup() error

	StartVideo() error
	AcceptVideo() error
	StopVideo() error
	SetVideoEnabled(enabled bool) error
	SetVideoOrientation(orientation int) error
	// SendVideo sends one already-encoded H.264 Annex-B access unit. The gateway
	// never encodes video; the access units come from the backend.
	SendVideo(accessUnit []byte, duration time.Duration) error

	SendReaction(emoji string) error
	SetHandRaised(raised bool) error
	StartScreenShare(id *uint32) error
	StopScreenShare() error

	AddParticipant(ctx context.Context, target string) error
	RingParticipant(ctx context.Context, target string) error
	SetApprovalRequired(ctx context.Context, enabled bool) error
	AdmitParticipant(ctx context.Context, user string) error
	DenyParticipant(ctx context.Context, user string) error

	// Play subscribes src as the call's outbound audio, as s16le mono 16 kHz
	// PCM. It is called exactly once per call, by Manager.Track, and the
	// source it is given lasts the whole call: the underlying library replaces
	// its player on every Play and drops the previous one without stopping it,
	// so a second caller would silently orphan the first. src is read
	// synchronously by the library's transmit loop, once per frame interval,
	// and must never block -- see outbound.go.
	Play(src io.ReadCloser) error
	// Receive attaches a sink for the peer's decoded audio frames.
	Receive(sink func(frame []float32))
	// ReceiveVideo attaches a sink for the peer's H.264 access units.
	ReceiveVideo(sink func(accessUnit []byte))

	OnReady(fn func())
	OnEnd(fn func(reason string))
	OnStateChange(fn func(phase Phase))
	OnPeerAccept(fn func())
	OnMuteState(fn func(muted bool))
	OnVideoState(fn func(VideoState))
	OnReaction(fn func(Reaction))
	OnGroupState(fn func(GroupState))
	OnWaitingRoomState(fn func(WaitingRoom))
	OnHandRaise(fn func(HandState))
	OnScreenShare(fn func(ScreenShare))
	// OnVideoKeyframeRequest fires on authenticated WhatsApp PLI/FIR feedback:
	// the peer's decoder needs a fresh IDR from our outgoing video, typically
	// after packet loss. The gateway never encodes video itself, so this only
	// ever has anywhere to go when an operator is attached -- see
	// Stream.Keyframe.
	OnVideoKeyframeRequest(fn func())
}

// RecordingStore is where finished recordings go. It takes a reader rather than
// a byte slice because a long call's audio runs to hundreds of megabytes.
type RecordingStore interface {
	PutStream(ctx context.Context, key, mime string, r io.Reader) error
}
