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

// Publisher carries call events to the backend.
type Publisher interface {
	PublishCall(ctx context.Context, evt Event) error
	// PublishInbound puts an arriving call onto the same routing key inbound
	// messages use, so the backend's existing message pipeline creates its
	// chat row.
	PublishInbound(ctx context.Context, evt InboundCallEvent) error
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
	// senderResolver reaches the channel's own client for its PNForLID store
	// lookup -- the same lookup an inbound message's sender resolution uses.
	// A channel with no live client yet (or gone) may return a nil resolver;
	// senderid.Resolve treats that as "no known phone number" rather than
	// failing the call.
	senderResolver func(channelID string) senderid.Resolver
	// avatars reaches the channel's own client for the peer's profile photo,
	// bound to that channel and its tenant. Like senderResolver it may return
	// nil for a channel with no live client, and may itself be nil.
	avatars  func(channelID string) AvatarSource
	opts     Options
	log      *slog.Logger
	registry *Registry

	// uploadWG tracks recording uploads still running off the teardown path,
	// so shutdown can wait for them instead of killing an upload that is
	// nearly done.
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
		// The inbound event goes out before incoming: the backend needs the
		// chat row to exist before any lifecycle transition -- accepted can
		// follow within milliseconds on a fast answer -- arrives to update it.
		m.publishInbound(t)
		m.publish(m.event(t, EventIncoming))
		m.flushEarlyEnd(t)
	})
}

// Get returns a live call, or false.
func (m *Manager) Get(channelID, callID string) (*Tracked, bool) {
	return m.registry.Get(channelID, callID)
}

// Track wires every callback on a call and registers it. Inbound calls come
// here from Attach, outbound ones from the command dispatcher.
//
// Callers must call flushEarlyEnd on the result once they have published the
// call's arrival: a call can end inside Track, and that end is deliberately
// held back rather than reported before anything knows the call exists.
func (m *Manager) Track(channelID string, lc LiveCall, direction string, record bool) *Tracked {
	// The identity lookup below can hit the store, and a call can end while it
	// is running -- a peer that cancels immediately, a rejected offer. The
	// library does not replay an end to a callback registered after the fact,
	// so an end landing in that window would leave the call registered
	// forever, never reported and never cleaned up. Catching it costs one
	// throwaway handler, which m.subscribe replaces below.
	var endedEarly atomic.Pointer[string]
	lc.OnEnd(func(reason string) { endedEarly.Store(&reason) })

	peer := lc.Peer()
	peerJID := m.parsePeerJID(channelID, peer)
	senderLid, senderPn := m.resolveSenderIdentity(channelID, peerJID)

	// A peer that resolved to neither identifier is one the gateway could not
	// make sense of, and there is nobody to ask WhatsApp about.
	var picture *avatar.Picture
	if direction == DirectionInbound && (senderLid != "" || senderPn != "") {
		picture = m.resolveProfilePicture(channelID, peerJID)
	}

	// The recorder is built before the outbound source because the source taps
	// it: the operator's half of the recording is taken from the one buffer the
	// gateway transmits through, not intercepted separately upstream.
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

	// Subscribing the call's outbound audio here, once, is what makes the
	// player slot unambiguous: the library's Play replaces the previous player
	// without stopping it, so a second caller silently orphans the first. Both
	// producers -- an attached operator's microphone and the play command --
	// write into this one source instead. See outbound.go for why it can never
	// block the library's send loop.
	if err := lc.Play(t.outbound); err != nil {
		m.log.Error("call: subscribe outbound audio",
			"channel_id", channelID, "call_id", t.CallID, "error", err)
	}

	// Both sinks always fan out to the recorder (when recording is on) and to
	// an attached stream (when an operator is listening in): the two are
	// independent, so either can be present without the other.
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

	// This is the only proof the library's incoming-call handler actually
	// fired: whatsmeow calls it silently, so a nil OnIncomingCall and a
	// handler that ran and published look identical unless this line exists.
	m.log.Info("call: tracking",
		"channel_id", t.ChannelID, "call_id", t.CallID, "direction", t.Direction, "is_video", t.IsVideo)

	t.earlyEnd = endedEarly.Load()
	return t
}

// flushEarlyEnd reports an end that landed while Track was still wiring the
// call up, which the library will not replay to the handler registered after
// it. Both of Track's callers must call it, and only once they have published
// the call's arrival: the backend has to know a call exists before it can be
// told the call is over.
func (m *Manager) flushEarlyEnd(t *Tracked) {
	if t.earlyEnd == nil {
		return
	}
	m.log.Warn("call: ended before tracking finished wiring it up",
		"channel_id", t.ChannelID, "call_id", t.CallID, "reason", *t.earlyEnd)
	m.end(context.Background(), t, *t.earlyEnd)
}

// parsePeerJID reads the peer the calling library handed us as a plain
// string. It is a string precisely so this package does not have to trust it
// as a well-formed JID; one it cannot parse is not fatal to tracking the
// call, only to saying who the call is with.
// A peer it cannot read yields the zero JID, which resolves to no identity
// and no photo rather than failing the call.
func (m *Manager) parsePeerJID(channelID, peer string) types.JID {
	jid, err := types.ParseJID(peer)
	if err != nil {
		m.log.Warn("call: peer is not a parseable JID", "channel_id", channelID, "peer", peer, "error", err)
		return types.JID{}
	}
	return jid
}

// resolveSenderIdentity turns a call's peer JID into the same
// senderLid/senderPn pair an inbound message from the same person carries,
// so the backend's contact lookup -- keyed on those strings -- finds one
// contact regardless of which event told it about them. It runs once, at
// Track time, rather than on every event a call goes on to publish: the
// PNForLID lookup behind it can hit the store, and a call's state can change
// many times before it ends.
func (m *Manager) resolveSenderIdentity(channelID string, jid types.JID) (senderLid, senderPn string) {
	var resolver senderid.Resolver
	if m.senderResolver != nil {
		resolver = m.senderResolver(channelID)
	}

	// A call's peer carries no alternate-identity hint the way a message's
	// Info.SenderAlt does -- the calling library only ever gives us the one
	// JID -- so the zero JID here always sends Resolve to the resolver
	// fallback for a @lid peer.
	return senderid.Resolve(context.Background(), resolver, jid, types.JID{})
}

// resolveProfilePicture fetches the peer's photo, for the same reason and at
// the same moment as their identity: once per call, at Track time, so a call
// that changes state a dozen times costs one lookup rather than a dozen.
//
// Only a call arriving resolves one. On a call we placed, the peer is a
// contact the backend already knows -- it just told us to dial them -- so the
// photo would be a round trip for something already on file.
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

// finish publishes the ended event and, if the call was recording, kicks off
// the upload in the background. It runs at most once per call, whichever
// path reaches it first.
func (m *Manager) finish(ctx context.Context, t *Tracked, reason string) {
	t.endOnce.Do(func() {
		// A stream outlives its call otherwise: nothing else tells an
		// operator parked on Audio()/Video() that the call is gone, so those
		// channels would simply never receive again instead of closing. This
		// also refuses any attach still in flight, which would otherwise store
		// a stream on a call nothing will ever come back to close.
		if s := t.finishStream(); s != nil {
			s.close()
		}
		// Nothing transmits on a call that is over; releasing the source also
		// tells the library's player to go idle the next time it reads.
		_ = t.outbound.Close()

		evt := m.event(t, EventEnded)
		evt.Reason = reason
		if !t.AnsweredAt.IsZero() {
			evt.Duration = int(m.opts.Now().Sub(t.AnsweredAt).Seconds())
		}

		// ended carries no media: a slow or unavailable object store must
		// never delay -- or, on a long enough stall, effectively withhold --
		// the event that tells the backend the call is over. The recording is
		// a by-product of the call, not a gate on its lifecycle.
		m.publish(evt)

		if t.Recorder != nil {
			m.uploadRecording(ctx, t)
		}
	})
}

// uploadRecording finishes and uploads a call's recording off the call's
// critical path, then reports the result as its own event. uploadWG lets
// shutdown wait for this to land instead of the process exiting mid-upload.
func (m *Manager) uploadRecording(ctx context.Context, t *Tracked) {
	// The caller's context may be a request context that is cancelled the
	// moment the handler returns, or (during shutdown) one tied to the
	// process exiting -- neither may abort an upload that is close to done.
	// WaitForRecordings, not context cancellation, is what bounds this.
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
			// A failed upload still has to reach the backend -- it must not
			// be silently swallowed just because it now runs off the main
			// path.
			m.log.Error("call: recording upload failed",
				"channel_id", t.ChannelID, "call_id", t.CallID, "error", err)
			failure := m.event(t, EventCommandFailed)
			failure.Error = &EventError{Code: CodeRecordingUploadFailed, Reason: err.Error()}
			m.publish(failure)
		}
	}()
}

// WaitForRecordings blocks until every recording upload still in flight
// finishes, or until timeout elapses, whichever comes first. Call it during
// shutdown, after AbortAll: without it, a process exit races an upload that
// is nearly done and the recording is lost even though the call otherwise
// completed cleanly.
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

// event builds the common shape of every event for a call.
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

// publishInbound puts a call -- arriving or placed -- onto the inbound message
// stream, shaped as a type: "call" message so the backend's existing pipeline
// creates the chat row for it. Like publish, it absorbs both publish errors
// and panics: this also runs on the calling library's media goroutine.
func (m *Manager) publishInbound(t *Tracked) {
	defer func() {
		if r := recover(); r != nil {
			m.log.Error("call: panic while publishing inbound call event",
				"channel_id", t.ChannelID, "call_id", t.CallID, "panic", r)
		}
	}()

	id := m.identity(t.ChannelID)
	// fromMe follows direction, not who t.SenderLid/SenderPn describe: those
	// stay the peer's identity either way, exactly as mapper.BuildInbound
	// resolves a from-me message's identity from the chat rather than from our
	// own device. fromMe only tells the backend which side placed the call.
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
