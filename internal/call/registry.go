package call

import (
	"sync"
	"time"

	"github.com/w3nder/whatsmeow-gateway/internal/avatar"
)

// Tracked is one call the gateway is following, with everything needed to
// report it and to act on it.
//
// A channel can hold more than one at a time -- a group call alongside a 1:1 --
// so the registry keys by call, never by channel alone.
type Tracked struct {
	CallID    string
	ChannelID string
	Direction string
	Peer      string
	// SenderLid and SenderPn are Peer resolved once, at Track time, the same
	// way an inbound message's sender is resolved -- every event this call
	// goes on to publish reads them from here rather than re-resolving on
	// each publish, which would repeat a store lookup on every state change
	// of a single call.
	SenderLid string
	SenderPn  string
	// ProfilePicture is the peer's photo, resolved alongside their identity
	// and for the same reason: once per call rather than once per event. Nil
	// on a call we placed, on a peer with no reachable photo, and whenever
	// the lookup did not work out.
	ProfilePicture *avatar.Picture
	IsVideo        bool

	// Live is the underlying call, used by the command dispatcher.
	Live LiveCall
	// Recorder is nil when recording is off for this call.
	Recorder *Recorder

	// earlyEnd carries an end reason that arrived while Track was still wiring
	// the call up, for Manager.flushEarlyEnd to report once the call's arrival
	// has been published. It is written once, by Track, before anything else
	// can reach this Tracked, and only read afterwards.
	earlyEnd *string

	// outbound is everything this call transmits to the peer: the operator's
	// microphone and the play command's announcements, merged behind one
	// non-blocking source that Manager.Track hands to Play exactly once. It
	// outlives every attach and detach, because the calling library's send
	// loop reads it for the whole life of the call.
	outbound *outboundAudio

	// streamMu guards Stream and finished, which an operator can change at any
	// point in the call's life, concurrently with the media goroutine reading
	// Stream on every frame.
	streamMu sync.Mutex
	// Stream is nil until an operator attaches; AttachStream and detach are
	// the only writers.
	Stream *Stream
	// finished marks the call as torn down. It closes the window between an
	// attach checking the registry and storing its stream: without it, a call
	// that ends inside that window leaves a stream nobody owns and nobody
	// closes, and an operator socket that never sees the end of the call.
	finished bool

	StartedAt time.Time
	// AnsweredAt is zero until media starts. Talk time is measured from here,
	// not from the offer: a call that rang for 40s and talked for 10s is a 10s
	// call.
	AnsweredAt time.Time

	// acceptOnce keeps the accepted event to one, since a call can both go
	// ready and report the peer accepting.
	acceptOnce sync.Once
	// endOnce keeps teardown to one, whether it comes from the library's end
	// callback or from a channel abort.
	endOnce sync.Once
}

// currentStream returns the attached stream, or nil.
func (t *Tracked) currentStream() *Stream {
	t.streamMu.Lock()
	defer t.streamMu.Unlock()
	return t.Stream
}

// setStream attaches s, returning whatever stream was attached before it (so
// the caller can close it) and whether the attach happened at all. It refuses
// once the call has been finished: teardown has already run and would never
// come back for this stream, so storing it would strand the operator on a
// socket that never ends.
func (t *Tracked) setStream(s *Stream) (old *Stream, ok bool) {
	t.streamMu.Lock()
	defer t.streamMu.Unlock()
	if t.finished {
		return nil, false
	}
	old = t.Stream
	t.Stream = s
	return old, true
}

// finishStream removes and returns whatever stream is attached, or nil, and
// closes the call to any further attach. Used when the call itself ends:
// whoever is parked on Audio()/Video() must see the channels close rather than
// hang forever on a call that is gone, and an attach still in flight must be
// refused rather than land on a call that is already torn down.
func (t *Tracked) finishStream() *Stream {
	t.streamMu.Lock()
	s := t.Stream
	t.Stream = nil
	t.finished = true
	t.streamMu.Unlock()
	return s
}

// clearStream detaches s, but only if it is still the attached stream: a
// stale detach (from a stream that AttachStream already replaced) must not
// clear the one that replaced it. It reports whether it actually detached, so
// the caller can tell an operator really leaving from a stale defer.
func (t *Tracked) clearStream(s *Stream) bool {
	t.streamMu.Lock()
	defer t.streamMu.Unlock()
	if t.Stream != s {
		return false
	}
	t.Stream = nil
	return true
}

// Registry holds the live calls of every channel on this instance.
type Registry struct {
	mu    sync.Mutex
	calls map[string]map[string]*Tracked
}

func NewRegistry() *Registry {
	return &Registry{calls: make(map[string]map[string]*Tracked)}
}

// Insert registers a call, replacing any earlier one with the same id.
func (r *Registry) Insert(t *Tracked) {
	r.mu.Lock()
	defer r.mu.Unlock()

	channel, ok := r.calls[t.ChannelID]
	if !ok {
		channel = make(map[string]*Tracked)
		r.calls[t.ChannelID] = channel
	}
	channel[t.CallID] = t
}

// Get returns a live call, or false.
func (r *Registry) Get(channelID, callID string) (*Tracked, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	t, ok := r.calls[channelID][callID]
	return t, ok
}

// Remove drops a call, reporting whether it was there.
func (r *Registry) Remove(channelID, callID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	channel, ok := r.calls[channelID]
	if !ok {
		return false
	}
	if _, ok := channel[callID]; !ok {
		return false
	}
	delete(channel, callID)
	if len(channel) == 0 {
		delete(r.calls, channelID)
	}
	return true
}

// TakeChannel removes and returns every call on a channel. Teardown takes them
// all at once so a call cannot be ended twice by a concurrent abort.
func (r *Registry) TakeChannel(channelID string) []*Tracked {
	r.mu.Lock()
	defer r.mu.Unlock()

	channel, ok := r.calls[channelID]
	if !ok {
		return nil
	}
	delete(r.calls, channelID)

	taken := make([]*Tracked, 0, len(channel))
	for _, t := range channel {
		taken = append(taken, t)
	}
	return taken
}

// Channels lists the channels holding at least one live call.
func (r *Registry) Channels() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	channels := make([]string, 0, len(r.calls))
	for channelID := range r.calls {
		channels = append(channels, channelID)
	}
	return channels
}
