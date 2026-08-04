package session

import (
	"context"
	"io"
	"time"

	"github.com/purpshell/meowcaller"
	"go.mau.fi/whatsmeow/types"

	"github.com/w3nder/whatsmeow-gateway/internal/call"
)

// This file is the only place that knows the calling library's concrete types.
// Everything else in the gateway works against call.Caller and call.LiveCall,
// which can be faked in a test; the library's own types cannot, because they
// are driven by a live VoIP stack.

// callerAdapter presents a meowcaller client as a call.Caller.
type callerAdapter struct {
	client *meowcaller.Client
}

func newCallerAdapter(client *meowcaller.Client) call.Caller {
	return &callerAdapter{client: client}
}

func (a *callerAdapter) Call(ctx context.Context, target string, video bool) (call.LiveCall, error) {
	c, err := a.client.CallWithOptions(ctx, target, meowcaller.CallOptions{Video: video})
	if err != nil {
		return nil, err
	}
	return &liveCallAdapter{call: c}, nil
}

func (a *callerAdapter) GroupCall(ctx context.Context, targets []string, opts call.GroupOptions) (call.LiveCall, error) {
	c, err := a.client.GroupCallWithOptions(ctx, targets, meowcaller.GroupCallOptions{
		GroupJID: opts.GroupJID,
		Video:    opts.Video,
	})
	if err != nil {
		return nil, err
	}
	return &liveCallAdapter{call: c}, nil
}

func (a *callerAdapter) GroupCallByID(ctx context.Context, groupID string, opts call.GroupOptions) (call.LiveCall, error) {
	c, err := a.client.GroupCallByIDWithOptions(ctx, groupID, meowcaller.GroupCallOptions{Video: opts.Video})
	if err != nil {
		return nil, err
	}
	return &liveCallAdapter{call: c}, nil
}

func (a *callerAdapter) CreateCallLink(ctx context.Context, video bool) (call.Link, error) {
	link, err := a.client.CreateCallLink(ctx, meowcaller.CallLinkOptions{Video: video})
	if err != nil {
		return call.Link{}, err
	}
	return call.Link{Token: link.Token, URL: link.URL, Video: link.Video}, nil
}

func (a *callerAdapter) PreviewCallLink(ctx context.Context, tokenOrURL string, video bool) (call.LinkPreview, error) {
	preview, err := a.client.PreviewCallLink(ctx, tokenOrURL, meowcaller.CallLinkOptions{Video: video})
	if err != nil {
		return call.LinkPreview{}, err
	}
	return call.LinkPreview{
		Token:            preview.Token,
		Video:            preview.Video,
		ApprovalRequired: preview.ApprovalRequired,
		IsAdmin:          preview.IsAdmin,
		Creator:          jidString(preview.Creator),
		CreatorPN:        jidString(preview.CreatorPhoneNumber),
	}, nil
}

func (a *callerAdapter) JoinCallLink(ctx context.Context, tokenOrURL string, video bool) (call.LiveCall, error) {
	c, err := a.client.JoinCallLink(ctx, tokenOrURL, meowcaller.CallLinkOptions{Video: video})
	if err != nil {
		return nil, err
	}
	return &liveCallAdapter{call: c}, nil
}

func (a *callerAdapter) OnIncomingCall(fn func(call.LiveCall)) {
	a.client.OnIncomingCall(func(c *meowcaller.Call) {
		fn(&liveCallAdapter{call: c})
	})
}

// liveCallAdapter presents a meowcaller call as a call.LiveCall.
type liveCallAdapter struct {
	call *meowcaller.Call
}

func (c *liveCallAdapter) ID() string    { return c.call.ID() }
func (c *liveCallAdapter) Peer() string  { return jidString(c.call.Peer()) }
func (c *liveCallAdapter) IsVideo() bool { return c.call.IsVideo() }

func (c *liveCallAdapter) Answer() error { return c.call.Answer() }
func (c *liveCallAdapter) Reject() error { return c.call.Reject() }
func (c *liveCallAdapter) Hangup() error { return c.call.Hangup() }

func (c *liveCallAdapter) StartVideo() error  { return c.call.StartVideo() }
func (c *liveCallAdapter) AcceptVideo() error { return c.call.AcceptVideo() }
func (c *liveCallAdapter) StopVideo() error   { return c.call.StopVideo() }

func (c *liveCallAdapter) SetVideoEnabled(enabled bool) error {
	return c.call.SetVideoEnabled(enabled)
}

func (c *liveCallAdapter) SetVideoOrientation(orientation int) error {
	return c.call.SetVideoOrientation(orientation)
}

func (c *liveCallAdapter) SendVideo(accessUnit []byte, duration time.Duration) error {
	return c.call.SendVideoWithDuration(accessUnit, duration)
}

func (c *liveCallAdapter) SendReaction(emoji string) error { return c.call.SendReaction(emoji) }
func (c *liveCallAdapter) SetHandRaised(raised bool) error { return c.call.SetHandRaised(raised) }

func (c *liveCallAdapter) StartScreenShare(id *uint32) error { return c.call.StartScreenShare(id) }
func (c *liveCallAdapter) StopScreenShare() error            { return c.call.StopScreenShare() }

func (c *liveCallAdapter) AddParticipant(ctx context.Context, target string) error {
	return c.call.AddParticipant(ctx, target)
}

func (c *liveCallAdapter) RingParticipant(ctx context.Context, target string) error {
	return c.call.RingParticipant(ctx, target)
}

func (c *liveCallAdapter) SetApprovalRequired(ctx context.Context, enabled bool) error {
	return c.call.SetApprovalRequired(ctx, enabled)
}

func (c *liveCallAdapter) AdmitParticipant(ctx context.Context, user string) error {
	return c.call.AdmitParticipant(ctx, user)
}

func (c *liveCallAdapter) DenyParticipant(ctx context.Context, user string) error {
	return c.call.DenyParticipant(ctx, user)
}

// Play streams s16le mono 16 kHz PCM. The gateway hands over raw PCM rather
// than an encoded file so the decoding stays on the backend's side.
func (c *liveCallAdapter) Play(src io.ReadCloser) error {
	c.call.Play(meowcaller.PCMStream(src))
	return nil
}

func (c *liveCallAdapter) Receive(sink func(frame []float32)) {
	c.call.Receive(meowcaller.SinkFunc(sink))
}

func (c *liveCallAdapter) ReceiveVideo(sink func(accessUnit []byte)) {
	c.call.ReceiveVideo(meowcaller.VideoSinkFunc(sink))
}

func (c *liveCallAdapter) OnReady(fn func())               { c.call.OnReady(fn) }
func (c *liveCallAdapter) OnEnd(fn func(reason string))    { c.call.OnEnd(fn) }
func (c *liveCallAdapter) OnPeerAccept(fn func())          { c.call.OnPeerAccept(fn) }
func (c *liveCallAdapter) OnMuteState(fn func(muted bool)) { c.call.OnMuteState(fn) }

func (c *liveCallAdapter) OnStateChange(fn func(call.Phase)) {
	c.call.OnStateChange(func(p meowcaller.CallPhase) { fn(phaseFrom(p)) })
}

func (c *liveCallAdapter) OnVideoState(fn func(call.VideoState)) {
	c.call.OnVideoState(func(v meowcaller.VideoState) { fn(videoStateFrom(v)) })
}

func (c *liveCallAdapter) OnReaction(fn func(call.Reaction)) {
	c.call.OnReaction(func(r meowcaller.CallReaction) { fn(reactionFrom(r)) })
}

func (c *liveCallAdapter) OnGroupState(fn func(call.GroupState)) {
	c.call.OnGroupState(func(g meowcaller.GroupCallState) { fn(groupStateFrom(g)) })
}

func (c *liveCallAdapter) OnWaitingRoomState(fn func(call.WaitingRoom)) {
	c.call.OnWaitingRoomState(func(w meowcaller.WaitingRoomState) { fn(waitingRoomFrom(w)) })
}

func (c *liveCallAdapter) OnHandRaise(fn func(call.HandState)) {
	c.call.OnHandRaise(func(h meowcaller.HandRaiseState) { fn(handStateFrom(h)) })
}

func (c *liveCallAdapter) OnScreenShare(fn func(call.ScreenShare)) {
	c.call.OnScreenShare(func(s meowcaller.ScreenShareState) { fn(screenShareFrom(s)) })
}

var _ call.LiveCall = (*liveCallAdapter)(nil)

// The conversions below flatten the library's types onto the gateway's own.
// JIDs become strings because that is what goes on the wire.

func phaseFrom(p meowcaller.CallPhase) call.Phase {
	switch p {
	case meowcaller.CallPhaseIdle:
		return call.PhaseIdle
	case meowcaller.CallPhaseCalling:
		return call.PhaseCalling
	case meowcaller.CallPhaseRinging:
		return call.PhaseRinging
	case meowcaller.CallPhaseConnecting:
		return call.PhaseConnecting
	case meowcaller.CallPhaseActive:
		return call.PhaseActive
	case meowcaller.CallPhaseEnded:
		return call.PhaseEnded
	case meowcaller.CallPhaseWaitingRoom:
		return call.PhaseWaitingRoom
	default:
		return call.PhaseIdle
	}
}

func videoStateFrom(v meowcaller.VideoState) call.VideoState {
	return call.VideoState{
		Active:      v.Active,
		Upgrade:     v.Upgrade,
		Orientation: v.Orientation,
		Raw:         v.Raw,
	}
}

func reactionFrom(r meowcaller.CallReaction) call.Reaction {
	return call.Reaction{
		Emoji:         r.Emoji,
		ParticipantID: r.ParticipantID,
		Sender:        jidString(r.Sender),
		Removed:       r.Removed,
	}
}

func groupStateFrom(g meowcaller.GroupCallState) call.GroupState {
	participants := make([]call.GroupParticipant, 0, len(g.Participants))
	for _, p := range g.Participants {
		participants = append(participants, call.GroupParticipant{
			JID:        jidString(p.JID),
			PN:         jidString(p.PN),
			State:      p.State,
			HandRaised: p.HandRaised,
		})
	}
	return call.GroupState{TransactionID: g.TransactionID, Participants: participants}
}

func waitingRoomFrom(w meowcaller.WaitingRoomState) call.WaitingRoom {
	users := make([]call.WaitingRoomUser, 0, len(w.Users))
	for _, u := range w.Users {
		users = append(users, call.WaitingRoomUser{
			JID:   jidString(u.JID),
			PN:    jidString(u.PN),
			State: u.State,
		})
	}
	return call.WaitingRoom{
		Enabled:       w.Enabled,
		IsAdmin:       w.IsAdmin,
		InWaitingRoom: w.InWaitingRoom,
		TransactionID: w.TransactionID,
		Users:         users,
	}
}

func handStateFrom(h meowcaller.HandRaiseState) call.HandState {
	return call.HandState{Participant: jidString(h.Participant), Raised: h.Raised}
}

func screenShareFrom(s meowcaller.ScreenShareState) call.ScreenShare {
	return call.ScreenShare{
		Participant: jidString(s.Participant),
		Active:      s.Active,
		Version:     s.Version,
	}
}

// jidString renders a JID, keeping an empty one empty rather than letting it
// stringify to a bare "@server".
func jidString(jid types.JID) string {
	if jid.IsEmpty() {
		return ""
	}
	return jid.String()
}
