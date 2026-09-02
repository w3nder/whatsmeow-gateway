package call

import (
	"context"
	"io"
	"time"
)

type GroupOptions struct {
	GroupJID string
	Video    bool
}

type Link struct {
	Token string
	URL   string
	Video bool
}

type LinkPreview struct {
	Token            string
	Video            bool
	ApprovalRequired bool
	IsAdmin          bool
	Creator          string
	CreatorPN        string
}

type Caller interface {
	Call(ctx context.Context, target string, video bool) (LiveCall, error)
	GroupCall(ctx context.Context, targets []string, opts GroupOptions) (LiveCall, error)
	GroupCallByID(ctx context.Context, groupID string, opts GroupOptions) (LiveCall, error)
	CreateCallLink(ctx context.Context, video bool) (Link, error)
	PreviewCallLink(ctx context.Context, tokenOrURL string, video bool) (LinkPreview, error)
	JoinCallLink(ctx context.Context, tokenOrURL string, video bool) (LiveCall, error)
	OnIncomingCall(fn func(LiveCall))
}

type LiveCall interface {
	ID() string
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

	Play(src io.ReadCloser) error
	Receive(sink func(frame []float32))
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
	OnVideoKeyframeRequest(fn func())
}

type RecordingStore interface {
	PutStream(ctx context.Context, key, mime string, r io.Reader) error
}
