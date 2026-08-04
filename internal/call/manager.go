package call

import (
	"context"
	"log/slog"
	"strconv"
	"time"
)

// Publisher carries call events to the backend.
type Publisher interface {
	PublishCall(ctx context.Context, evt Event) error
}

// Identity is the channel-scoped identity stamped on every event, resolved at
// publish time because a channel's tenant and device are not known to this
// package.
type Identity struct {
	PhoneNumberID string
	TenantID      string
}

// Options configures the manager.
type Options struct {
	// TmpDir is where recordings are staged. Empty means the system temp dir.
	TmpDir string
	// Record turns recording on for every call by default; a command may still
	// turn it off per call.
	Record bool
	// Now is injected by tests. Nil means time.Now.
	Now func() time.Time
}

// Manager follows every live call on this instance: it subscribes to the
// calling library's callbacks, turns them into events, and owns the recording
// lifecycle.
type Manager struct {
	pub      Publisher
	store    RecordingStore
	identity func(channelID string) Identity
	opts     Options
	log      *slog.Logger
	registry *Registry
}

func NewManager(
	pub Publisher,
	store RecordingStore,
	identity func(channelID string) Identity,
	opts Options,
	log *slog.Logger,
) *Manager {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Manager{
		pub:      pub,
		store:    store,
		identity: identity,
		opts:     opts,
		log:      log,
		registry: NewRegistry(),
	}
}

// Attach subscribes to a channel's inbound calls. Called once per live session.
//
// A nil caller is a no-op: a client built without a calling stack simply never
// reports a call, which must not be fatal to the channel.
func (m *Manager) Attach(channelID string, caller Caller) {
	if caller == nil {
		return
	}

	caller.OnIncomingCall(func(lc LiveCall) {
		t := m.Track(channelID, lc, DirectionInbound, m.opts.Record)
		m.publish(m.event(t, EventIncoming))
	})
}

// Get returns a live call, or false.
func (m *Manager) Get(channelID, callID string) (*Tracked, bool) {
	return m.registry.Get(channelID, callID)
}

// Track wires every callback on a call and registers it. Inbound calls come
// here from Attach, outbound ones from the command dispatcher.
func (m *Manager) Track(channelID string, lc LiveCall, direction string, record bool) *Tracked {
	t := &Tracked{
		CallID:    lc.ID(),
		ChannelID: channelID,
		Direction: direction,
		Peer:      lc.Peer(),
		IsVideo:   lc.IsVideo(),
		Live:      lc,
		StartedAt: m.opts.Now(),
	}
	if record {
		t.Recorder = NewRecorder(m.opts.TmpDir, channelID, t.CallID)
	}

	// Both sinks always fan out to the recorder (when recording is on) and to
	// an attached stream (when an operator is listening in): the two are
	// independent, so either can be present without the other.
	lc.Receive(func(frame []float32) {
		if t.Recorder != nil {
			t.Recorder.WriteAudio(frame)
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

	// This is the only proof the library's incoming-call handler actually
	// fired: whatsmeow calls it silently, so a nil OnIncomingCall and a
	// handler that ran and published look identical unless this line exists.
	m.log.Info("call: tracking",
		"channel_id", t.ChannelID, "call_id", t.CallID, "direction", t.Direction, "is_video", t.IsVideo)

	return t
}

// subscribe wires the library's callbacks. Everything published here goes
// through m.publish, which absorbs panics: these fire on the library's media
// goroutines, where a panic would otherwise take the process down.
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

	// The peer asking for a fresh IDR only means anything when an operator's
	// encoder is attached to receive the request; with nothing attached
	// there is no one to tell.
	lc.OnVideoKeyframeRequest(func() {
		if s := t.currentStream(); s != nil {
			s.requestKeyframe()
		}
	})
}

// markAnswered stamps the answer time once. A call can both report media ready
// and report the peer accepting; only the first counts.
func (m *Manager) markAnswered(t *Tracked) {
	t.acceptOnce.Do(func() {
		t.AnsweredAt = m.opts.Now()
	})
}

// AbortChannel ends every live call on a channel, flushing recordings and
// reporting the reason. Called when a session logs out, drops or shuts down:
// media does not survive a dead socket, and leaving the call registered would
// hide its end from the backend.
func (m *Manager) AbortChannel(ctx context.Context, channelID, reason string) {
	for _, t := range m.registry.TakeChannel(channelID) {
		m.finish(ctx, t, reason)
	}
}

// AbortAll ends every live call on every channel.
func (m *Manager) AbortAll(ctx context.Context, reason string) {
	for _, channelID := range m.registry.Channels() {
		m.AbortChannel(ctx, channelID, reason)
	}
}

// end tears a call down from the library's own end callback.
func (m *Manager) end(ctx context.Context, t *Tracked, reason string) {
	if !m.registry.Remove(t.ChannelID, t.CallID) {
		// Already taken by a channel abort; that path finishes it.
		return
	}
	m.finish(ctx, t, reason)
}

// finish uploads the recording and publishes the ended event. It runs at most
// once per call, whichever path reaches it first.
func (m *Manager) finish(ctx context.Context, t *Tracked, reason string) {
	t.endOnce.Do(func() {
		// A stream outlives its call otherwise: nothing else tells an
		// operator parked on Audio()/Video() that the call is gone, so those
		// channels would simply never receive again instead of closing.
		if s := t.takeStream(); s != nil {
			s.close()
		}

		evt := m.event(t, EventEnded)
		evt.Reason = reason
		if !t.AnsweredAt.IsZero() {
			evt.Duration = int(m.opts.Now().Sub(t.AnsweredAt).Seconds())
		}

		var uploadErr error
		if t.Recorder != nil {
			audio, video, err := t.Recorder.Finish(ctx, m.store)
			evt.Media, evt.VideoMedia, uploadErr = audio, video, err
		}

		// The ended event goes out first. Losing a recording must not hide the
		// end of the call from the backend.
		m.publish(evt)

		if uploadErr != nil {
			m.log.Error("call: recording upload failed",
				"channel_id", t.ChannelID, "call_id", t.CallID, "error", uploadErr)
			failure := m.event(t, EventCommandFailed)
			failure.Error = &EventError{Code: CodeRecordingUploadFailed, Reason: uploadErr.Error()}
			m.publish(failure)
		}
	})
}

// event builds the common shape of every event for a call.
func (m *Manager) event(t *Tracked, eventType string) Event {
	id := m.identity(t.ChannelID)
	return Event{
		PhoneNumberID: id.PhoneNumberID,
		TenantID:      id.TenantID,
		ChannelID:     t.ChannelID,
		CallID:        t.CallID,
		From:          t.Peer,
		Direction:     t.Direction,
		Type:          eventType,
		Timestamp:     strconv.FormatInt(m.opts.Now().Unix(), 10),
		IsVideo:       t.IsVideo,
	}
}

// publish sends an event, absorbing both publish errors and panics. Callbacks
// run on the calling library's media goroutines, so a panic here would kill the
// process rather than just the call.
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

	// publish is the single choke point every event goes through, so this is
	// the one line that proves an event actually reached the broker rather
	// than silently never being built. State and video-state updates can fire
	// repeatedly through a single call, so those go to Debug; the lifecycle
	// events an operator debugging a call cares about stay at Info.
	level := slog.LevelInfo
	if evt.Type == EventState || evt.Type == EventVideo {
		level = slog.LevelDebug
	}
	m.log.Log(context.Background(), level, "call: event published",
		"channel_id", evt.ChannelID, "call_id", evt.CallID, "type", evt.Type)
}
