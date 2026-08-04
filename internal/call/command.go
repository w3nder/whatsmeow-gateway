package call

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
)

// MediaFetcher fetches the bytes behind a command's mediaUrl.
type MediaFetcher func(ctx context.Context, url string) ([]byte, error)

// Dispatch runs one command against a channel's caller.
//
// It always reports the outcome as an event carrying the command id, and it
// returns an error only when the failure is the caller's to retry. A malformed
// or impossible command is never an error: nacking it would put it back on the
// queue and loop it to the DLQ.
func (m *Manager) Dispatch(
	ctx context.Context,
	caller Caller,
	cmd amqp.GatewayCallCommand,
	fetch MediaFetcher,
) error {
	if caller == nil {
		m.failCommand(cmd, CodeNoCaller, "channel has no calling client")
		return nil
	}

	switch cmd.Action {
	case "dial", "group.dial", "group.dial_by_id", "link.create", "link.preview", "link.join":
		return m.dispatchCaller(ctx, caller, cmd)
	default:
		return m.dispatchLive(ctx, cmd, fetch)
	}
}

// dispatchCaller handles the actions that create a call or work on links, none
// of which need an existing call.
func (m *Manager) dispatchCaller(ctx context.Context, caller Caller, cmd amqp.GatewayCallCommand) error {
	switch cmd.Action {
	case "dial":
		if cmd.To == "" {
			m.failCommand(cmd, CodeInvalidTarget, "dial requires a target in \"to\"")
			return nil
		}
		lc, err := caller.Call(ctx, cmd.To, cmd.Video)
		if err != nil {
			m.failCommand(cmd, CodeActionFailed, err.Error())
			return nil
		}
		m.trackOutbound(cmd, lc)

	case "group.dial":
		// WhatsApp needs at least two remote parties for a group call; one
		// target is a 1:1 call wearing the wrong command.
		if len(cmd.Targets) < 2 {
			m.failCommand(cmd, CodeInvalidTarget, "group.dial requires at least two targets")
			return nil
		}
		lc, err := caller.GroupCall(ctx, cmd.Targets, GroupOptions{GroupJID: cmd.GroupID, Video: cmd.Video})
		if err != nil {
			m.failCommand(cmd, CodeActionFailed, err.Error())
			return nil
		}
		m.trackOutbound(cmd, lc)

	case "group.dial_by_id":
		if cmd.GroupID == "" {
			m.failCommand(cmd, CodeInvalidTarget, "group.dial_by_id requires a groupId")
			return nil
		}
		lc, err := caller.GroupCallByID(ctx, cmd.GroupID, GroupOptions{Video: cmd.Video})
		if err != nil {
			m.failCommand(cmd, CodeActionFailed, err.Error())
			return nil
		}
		m.trackOutbound(cmd, lc)

	case "link.create":
		link, err := caller.CreateCallLink(ctx, cmd.Video)
		if err != nil {
			m.failCommand(cmd, CodeActionFailed, err.Error())
			return nil
		}
		evt := m.commandEvent(cmd, EventLinkCreated)
		evt.Link = &EventLink{Token: link.Token, URL: link.URL, Video: link.Video}
		m.publish(evt)

	case "link.preview":
		if cmd.LinkToken == "" {
			m.failCommand(cmd, CodeInvalidTarget, "link.preview requires a linkToken")
			return nil
		}
		preview, err := caller.PreviewCallLink(ctx, cmd.LinkToken, cmd.Video)
		if err != nil {
			m.failCommand(cmd, CodeActionFailed, err.Error())
			return nil
		}
		evt := m.commandEvent(cmd, EventLinkPreview)
		evt.Link = &EventLink{
			Token:            preview.Token,
			Video:            preview.Video,
			ApprovalRequired: preview.ApprovalRequired,
			IsAdmin:          preview.IsAdmin,
		}
		m.publish(evt)

	case "link.join":
		if cmd.LinkToken == "" {
			m.failCommand(cmd, CodeInvalidTarget, "link.join requires a linkToken")
			return nil
		}
		lc, err := caller.JoinCallLink(ctx, cmd.LinkToken, cmd.Video)
		if err != nil {
			m.failCommand(cmd, CodeActionFailed, err.Error())
			return nil
		}
		m.trackOutbound(cmd, lc)
	}

	return nil
}

// trackOutbound registers a call the gateway just placed and reports it ringing.
func (m *Manager) trackOutbound(cmd amqp.GatewayCallCommand, lc LiveCall) {
	t := m.Track(cmd.ChannelID, lc, DirectionOutbound, m.recordFor(cmd))

	// The inbound event goes out before ringing, for the same reason Attach
	// publishes it before incoming: the backend needs the chat row to exist
	// before any lifecycle transition arrives to update it.
	m.publishInbound(t)

	evt := m.event(t, EventRinging)
	evt.CommandID = cmd.CommandID
	m.publish(evt)

	m.ackCommand(cmd)
}

// recordFor resolves whether a call records: the command may turn it off, but
// only the gateway default can turn it on.
func (m *Manager) recordFor(cmd amqp.GatewayCallCommand) bool {
	if cmd.Record != nil {
		return *cmd.Record && m.opts.Record
	}
	return m.opts.Record
}

// dispatchLive handles the actions that act on a call already in flight.
func (m *Manager) dispatchLive(ctx context.Context, cmd amqp.GatewayCallCommand, fetch MediaFetcher) error {
	tracked, ok := m.registry.Get(cmd.ChannelID, cmd.CallID)
	if !ok {
		if cmd.Action == "hangup" || cmd.Action == "reject" {
			// hangup and reject express a desired end state, not an action on
			// a specific live call. When the call is already gone -- an easy
			// race, since the front can send hangup the same moment the peer
			// hangs up -- that end state already holds, so the command is
			// satisfied rather than a fault.
			m.log.Info("call: command satisfied, call already ended",
				"channel_id", cmd.ChannelID, "call_id", cmd.CallID,
				"command_id", cmd.CommandID, "action", cmd.Action)
			m.ackCommand(cmd)
			return nil
		}
		m.failCommand(cmd, CodeCallNotFound,
			fmt.Sprintf("no live call %q on channel %q", cmd.CallID, cmd.ChannelID))
		return nil
	}
	lc := tracked.Live

	var err error
	switch cmd.Action {
	case "answer":
		err = lc.Answer()
	case "reject":
		err = lc.Reject()
	case "hangup":
		err = lc.Hangup()
	case "video.start":
		err = lc.StartVideo()
	case "video.accept":
		err = lc.AcceptVideo()
	case "video.stop":
		err = lc.StopVideo()
	case "video.enable":
		err = lc.SetVideoEnabled(cmd.Enabled)
	case "video.orientation":
		err = lc.SetVideoOrientation(cmd.Orientation)
	case "reaction":
		err = lc.SendReaction(cmd.Emoji)
	case "hand.raise":
		err = lc.SetHandRaised(cmd.Raised)
	case "screenshare.start":
		err = lc.StartScreenShare(nil)
	case "screenshare.stop":
		err = lc.StopScreenShare()
	case "participant.add":
		err = lc.AddParticipant(ctx, cmd.Participant)
	case "participant.ring":
		err = lc.RingParticipant(ctx, cmd.Participant)
	case "approval.set":
		err = lc.SetApprovalRequired(ctx, cmd.Enabled)
	case "participant.admit":
		err = lc.AdmitParticipant(ctx, cmd.Participant)
	case "participant.deny":
		err = lc.DenyParticipant(ctx, cmd.Participant)
	case "play":
		err = m.play(ctx, lc, cmd, fetch)
	case "video.play":
		err = m.playVideo(ctx, tracked, cmd, fetch)
	default:
		m.failCommand(cmd, CodeUnknownAction, fmt.Sprintf("unknown call action %q", cmd.Action))
		return nil
	}

	if err != nil {
		code := CodeActionFailed
		if isFetchError(err) {
			code = CodeMediaFetch
		}
		m.failCommand(cmd, code, err.Error())
		return nil
	}

	m.ackCommand(cmd)
	return nil
}

// fetchError marks a failure that came from fetching the command's media, so
// the backend can tell a bad URL from a call that refused the action.
type fetchError struct{ err error }

func (e fetchError) Error() string { return e.err.Error() }
func (e fetchError) Unwrap() error { return e.err }

func isFetchError(err error) bool {
	var fe fetchError
	return errors.As(err, &fe)
}

// commandEvent builds an event tied to a command rather than to a tracked call.
func (m *Manager) commandEvent(cmd amqp.GatewayCallCommand, eventType string) Event {
	id := m.identity(cmd.ChannelID)
	tenantID := id.TenantID
	if tenantID == "" {
		tenantID = cmd.TenantID
	}
	return Event{
		PhoneNumberID: id.PhoneNumberID,
		TenantID:      tenantID,
		ChannelID:     cmd.ChannelID,
		CallID:        cmd.CallID,
		CommandID:     cmd.CommandID,
		Direction:     DirectionOutbound,
		Type:          eventType,
		Timestamp:     strconv.FormatInt(m.opts.Now().Unix(), 10),
	}
}

func (m *Manager) ackCommand(cmd amqp.GatewayCallCommand) {
	m.publish(m.commandEvent(cmd, EventCommandAck))
}

func (m *Manager) failCommand(cmd amqp.GatewayCallCommand, code, reason string) {
	m.log.Error("call: command failed",
		"channel_id", cmd.ChannelID,
		"call_id", cmd.CallID,
		"command_id", cmd.CommandID,
		"action", cmd.Action,
		"code", code,
		"reason", reason)

	evt := m.commandEvent(cmd, EventCommandFailed)
	evt.Error = &EventError{Code: code, Reason: reason}
	m.publish(evt)
}

// play streams audio from the command's media URL into the call. The bytes are
// s16le mono 16 kHz PCM.
func (m *Manager) play(ctx context.Context, lc LiveCall, cmd amqp.GatewayCallCommand, fetch MediaFetcher) error {
	if cmd.MediaURL == "" {
		return fetchError{fmt.Errorf("call: play requires a mediaUrl")}
	}
	data, err := fetch(ctx, cmd.MediaURL)
	if err != nil {
		return fetchError{fmt.Errorf("call: fetch play media: %w", err)}
	}
	if len(data) == 0 {
		return fetchError{fmt.Errorf("call: play media %s is empty", cmd.MediaURL)}
	}
	return lc.Play(io.NopCloser(bytes.NewReader(data)))
}
