package call

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.mau.fi/whatsmeow/types"

	"github.com/w3nder/whatsmeow-gateway/internal/avatar"
	"github.com/w3nder/whatsmeow-gateway/internal/senderid"
)

type Publisher interface {
	PublishCall(ctx context.Context, evt Event) error
	PublishInbound(ctx context.Context, evt InboundCallEvent) error
}

type Identity struct {
	PhoneNumberID string
	TenantID      string
}

type Options struct {
	TmpDir string
	Record bool
	Now    func() time.Time
}

type Manager struct {
	pub            Publisher
	store          RecordingStore
	identity       func(channelID string) Identity
	senderResolver func(channelID string) senderid.Resolver
	avatars        func(channelID string) AvatarSource
	opts           Options
	log            *slog.Logger
	registry       *Registry

	uploadWG sync.WaitGroup
}

func NewManager(
	pub Publisher,
	store RecordingStore,
	identity func(channelID string) Identity,
	senderResolver func(channelID string) senderid.Resolver,
	avatars func(channelID string) AvatarSource,
	opts Options,
	log *slog.Logger,
) *Manager {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Manager{
		pub:            pub,
		store:          store,
		identity:       identity,
		senderResolver: senderResolver,
		avatars:        avatars,
		opts:           opts,
		log:            log,
		registry:       NewRegistry(),
	}
}

func (m *Manager) Attach(channelID string, caller Caller) {
	if caller == nil {
		return
	}

	caller.OnIncomingCall(func(lc LiveCall) {
		t := m.Track(channelID, lc, DirectionInbound, m.opts.Record)
		m.publishInbound(t)
		m.publish(m.event(t, EventIncoming))
		m.flushEarlyEnd(t)
	})
}

func (m *Manager) Get(channelID, callID string) (*Tracked, bool) {
	return m.registry.Get(channelID, callID)
}

func (m *Manager) Track(channelID string, lc LiveCall, direction string, record bool) *Tracked {
	var endedEarly atomic.Pointer[string]
	lc.OnEnd(func(reason string) { endedEarly.Store(&reason) })

	peer := lc.Peer()
	peerJID := m.parsePeerJID(channelID, peer)
	senderLid, senderPn := m.resolveSenderIdentity(channelID, peerJID)

	var picture *avatar.Picture
	if direction == DirectionInbound && (senderLid != "" || senderPn != "") {
		picture = m.resolveProfilePicture(channelID, peerJID)
	}

	var recorder *Recorder
	if record {
		recorder = NewRecorder(m.opts.TmpDir, channelID, lc.ID())
	}

	t := &Tracked{
		CallID:    lc.ID(),
		ChannelID: channelID,
		Direction: direction,
		Peer:      peer,
		SenderLid: senderLid,
		SenderPn:  senderPn,

		ProfilePicture: picture,

		IsVideo:   lc.IsVideo(),
		Live:      lc,
		Recorder:  recorder,
		outbound:  newOutboundAudio(recorder),
		StartedAt: m.opts.Now(),
	}

	if err := lc.Play(t.outbound); err != nil {
		m.log.Error("call: subscribe outbound audio",
			"channel_id", channelID, "call_id", t.CallID, "error", err)
	}

	lc.Receive(func(frame []float32) {
		if t.Recorder != nil {
			t.Recorder.WritePeerAudio(frame)
		}
		if s := t.currentStream(); s != nil {
			s.receiveAudio(frame)
		}
	})
	lc.ReceiveVideo(func(accessUnit []byte) {
		if t.Recorder != nil {
			t.Recorder.WriteVideo(accessUnit)
		}
		if s := t.currentStream(); s != nil {
			s.receiveVideo(accessUnit)
		}
	})

	m.registry.Insert(t)
	m.subscribe(t, lc)

	m.log.Info("call: tracking",
		"channel_id", t.ChannelID, "call_id", t.CallID, "direction", t.Direction, "is_video", t.IsVideo)

	t.earlyEnd = endedEarly.Load()
	return t
}

func (m *Manager) flushEarlyEnd(t *Tracked) {
	if t.earlyEnd == nil {
		return
	}
	m.log.Warn("call: ended before tracking finished wiring it up",
		"channel_id", t.ChannelID, "call_id", t.CallID, "reason", *t.earlyEnd)
	m.end(context.Background(), t, *t.earlyEnd)
}

func (m *Manager) parsePeerJID(channelID, peer string) types.JID {
	jid, err := types.ParseJID(peer)
	if err != nil {
		m.log.Warn("call: peer is not a parseable JID", "channel_id", channelID, "peer", peer, "error", err)
		return types.JID{}
	}
	return jid
}

func (m *Manager) resolveSenderIdentity(channelID string, jid types.JID) (senderLid, senderPn string) {
	var resolver senderid.Resolver
	if m.senderResolver != nil {
		resolver = m.senderResolver(channelID)
	}

	return senderid.Resolve(context.Background(), resolver, jid, types.JID{})
}

func (m *Manager) resolveProfilePicture(channelID string, jid types.JID) *avatar.Picture {
	if m.avatars == nil {
		return nil
	}
	source := m.avatars(channelID)
	if source == nil {
		return nil
	}
	return source.For(context.Background(), jid)
}

func (m *Manager) subscribe(t *Tracked, lc LiveCall) {
	lc.OnReady(func() {
		m.markAnswered(t)
		m.publish(m.event(t, EventAccepted))
	})

	lc.OnPeerAccept(func() {
		m.markAnswered(t)
		m.publish(m.event(t, EventAccepted))
	})

	lc.OnEnd(func(reason string) {
		m.end(context.Background(), t, reason)
	})

	lc.OnStateChange(func(phase Phase) {
		evt := m.event(t, EventState)
		evt.State = phase.String()
		m.publish(evt)
	})

	lc.OnMuteState(func(muted bool) {
		evt := m.event(t, EventState)
		evt.Muted = &muted
		m.publish(evt)
	})

	lc.OnVideoState(func(state VideoState) {
		evt := m.event(t, EventVideo)
		evt.Video = &EventVideoState{
			Active:      state.Active,
			Upgrade:     state.Upgrade,
			Orientation: state.Orientation,
		}
		m.publish(evt)
	})

	lc.OnReaction(func(r Reaction) {
		evt := m.event(t, EventReactionType)
		evt.Reaction = &EventReaction{Emoji: r.Emoji, Sender: r.Sender, Removed: r.Removed}
		m.publish(evt)
	})

	lc.OnGroupState(func(state GroupState) {
		evt := m.event(t, EventGroupState)
		evt.Group = &state
		m.publish(evt)
	})

	lc.OnWaitingRoomState(func(state WaitingRoom) {
		evt := m.event(t, EventWaitingRoom)
		evt.WaitingRoom = &state
		m.publish(evt)
	})

	lc.OnHandRaise(func(state HandState) {
		evt := m.event(t, EventHand)
		evt.Hand = &state
		m.publish(evt)
	})

	lc.OnScreenShare(func(state ScreenShare) {
		evt := m.event(t, EventScreenShare)
		evt.ScreenShare = &state
		m.publish(evt)
	})

	lc.OnVideoKeyframeRequest(func() {
		if s := t.currentStream(); s != nil {
			s.requestKeyframe()
		}
	})
}

func (m *Manager) markAnswered(t *Tracked) {
	t.acceptOnce.Do(func() {
		t.AnsweredAt = m.opts.Now()
	})
}

func (m *Manager) AbortChannel(ctx context.Context, channelID, reason string) {
	for _, t := range m.registry.TakeChannel(channelID) {
		m.finish(ctx, t, reason)
	}
}

func (m *Manager) AbortAll(ctx context.Context, reason string) {
	for _, channelID := range m.registry.Channels() {
		m.AbortChannel(ctx, channelID, reason)
	}
}

func (m *Manager) end(ctx context.Context, t *Tracked, reason string) {
	if !m.registry.Remove(t.ChannelID, t.CallID) {
		return
	}
	m.finish(ctx, t, reason)
}

func (m *Manager) finish(ctx context.Context, t *Tracked, reason string) {
	t.endOnce.Do(func() {
		if s := t.finishStream(); s != nil {
			s.close()
		}
		_ = t.outbound.Close()

		evt := m.event(t, EventEnded)
		evt.Reason = reason
		if !t.AnsweredAt.IsZero() {
			evt.Duration = int(m.opts.Now().Sub(t.AnsweredAt).Seconds())
		}

		m.publish(evt)

		if t.Recorder != nil {
			m.uploadRecording(ctx, t)
		}
	})
}

func (m *Manager) uploadRecording(ctx context.Context, t *Tracked) {
	uploadCtx := context.WithoutCancel(ctx)

	m.uploadWG.Add(1)
	go func() {
		defer m.uploadWG.Done()

		rec, err := t.Recorder.Finish(uploadCtx, m.store)
		if rec.Audio != nil || rec.Video != nil {
			evt := m.event(t, EventRecording)
			evt.Media, evt.VideoMedia = rec.Audio, rec.Video
			evt.PeerMedia, evt.OperatorMedia = rec.PeerAudio, rec.OperatorAudio
			m.publish(evt)
		}

		if err != nil {
			m.log.Error("call: recording upload failed",
				"channel_id", t.ChannelID, "call_id", t.CallID, "error", err)
			failure := m.event(t, EventCommandFailed)
			failure.Error = &EventError{Code: CodeRecordingUploadFailed, Reason: err.Error()}
			m.publish(failure)
		}
	}()
}

func (m *Manager) WaitForRecordings(timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		m.uploadWG.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		m.log.Error("call: shutdown timed out waiting for recording uploads", "timeout", timeout)
	}
}

func (m *Manager) event(t *Tracked, eventType string) Event {
	id := m.identity(t.ChannelID)
	return Event{
		PhoneNumberID: id.PhoneNumberID,
		TenantID:      id.TenantID,
		ChannelID:     t.ChannelID,
		CallID:        t.CallID,
		From:          senderid.From(t.SenderLid, t.SenderPn),
		SenderLid:     t.SenderLid,
		SenderPn:      t.SenderPn,
		Direction:     t.Direction,
		Type:          eventType,
		Timestamp:     strconv.FormatInt(m.opts.Now().Unix(), 10),
		IsVideo:       t.IsVideo,
	}
}

func (m *Manager) publishInbound(t *Tracked) {
	defer func() {
		if r := recover(); r != nil {
			m.log.Error("call: panic while publishing inbound call event",
				"channel_id", t.ChannelID, "call_id", t.CallID, "panic", r)
		}
	}()

	id := m.identity(t.ChannelID)
	fromMe := t.Direction == DirectionOutbound
	evt := NewInboundCallEvent(id, t.ChannelID, t.CallID, t.SenderLid, t.SenderPn, t.Direction, fromMe, t.IsVideo,
		strconv.FormatInt(m.opts.Now().Unix(), 10), t.ProfilePicture)

	if err := m.pub.PublishInbound(context.Background(), evt); err != nil {
		m.log.Error("call: publish inbound call event",
			"channel_id", t.ChannelID, "call_id", t.CallID, "error", err)
		return
	}

	m.log.Info("call: inbound call event published", "channel_id", t.ChannelID, "call_id", t.CallID)
}

func (m *Manager) publish(evt Event) {
	defer func() {
		if r := recover(); r != nil {
			m.log.Error("call: panic while publishing call event",
				"channel_id", evt.ChannelID, "call_id", evt.CallID, "type", evt.Type, "panic", r)
		}
	}()

	if err := m.pub.PublishCall(context.Background(), evt); err != nil {
		m.log.Error("call: publish call event",
			"channel_id", evt.ChannelID, "call_id", evt.CallID, "type", evt.Type, "error", err)
		return
	}

	level := slog.LevelInfo
	if evt.Type == EventState || evt.Type == EventVideo {
		level = slog.LevelDebug
	}
	m.log.Log(context.Background(), level, "call: event published",
		"channel_id", evt.ChannelID, "call_id", evt.CallID, "type", evt.Type)
}
