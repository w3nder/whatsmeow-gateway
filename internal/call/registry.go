package call

import (
	"sync"
	"time"
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
	IsVideo   bool

	// Live is the underlying call, used by the command dispatcher.
	Live LiveCall
	// Recorder is nil when recording is off for this call.
	Recorder *Recorder

	// streamMu guards Stream, which an operator can attach or detach at any
	// point in the call's life, concurrently with the media goroutine reading
	// it on every frame.
	streamMu sync.Mutex
	// Stream is nil until an operator attaches; AttachStream and detach are
	// the only writers.
	Stream *Stream

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

// setStream attaches s, returning whatever stream was attached before it so
// the caller can close it.
func (t *Tracked) setStream(s *Stream) *Stream {
	t.streamMu.Lock()
	old := t.Stream
	t.Stream = s
	t.streamMu.Unlock()
	return old
}

// clearStream detaches s, but only if it is still the attached stream: a
// stale detach (from a stream that AttachStream already replaced) must not
// clear the one that replaced it.
func (t *Tracked) clearStream(s *Stream) {
	t.streamMu.Lock()
	if t.Stream == s {
		t.Stream = nil
	}
	t.streamMu.Unlock()
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
