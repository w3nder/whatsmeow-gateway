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
// without a real call, and exactly one file (internal/session/caller.go) knows
// the library's concrete types.

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

	// Play streams s16le mono 16 kHz PCM to the peer.
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
}

// RecordingStore is where finished recordings go. It takes a reader rather than
// a byte slice because a long call's audio runs to hundreds of megabytes.
type RecordingStore interface {
	PutStream(ctx context.Context, key, mime string, r io.Reader) error
}
