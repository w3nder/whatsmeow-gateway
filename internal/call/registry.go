package call

import (
	"sync"
	"time"

	"github.com/w3nder/whatsmeow-gateway/internal/avatar"
)

type Tracked struct {
	CallID         string
	ChannelID      string
	Direction      string
	Peer           string
	SenderLid      string
	SenderPn       string
	ProfilePicture *avatar.Picture
	IsVideo        bool

	Live     LiveCall
	Recorder *Recorder

	earlyEnd *string

	outbound *outboundAudio

	streamMu sync.Mutex
	Stream   *Stream
	finished bool

	StartedAt  time.Time
	AnsweredAt time.Time

	acceptOnce sync.Once
	endOnce    sync.Once
}

func (t *Tracked) currentStream() *Stream {
	t.streamMu.Lock()
	defer t.streamMu.Unlock()
	return t.Stream
}

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

func (t *Tracked) finishStream() *Stream {
	t.streamMu.Lock()
	s := t.Stream
	t.Stream = nil
	t.finished = true
	t.streamMu.Unlock()
	return s
}

func (t *Tracked) clearStream(s *Stream) bool {
	t.streamMu.Lock()
	defer t.streamMu.Unlock()
	if t.Stream != s {
		return false
	}
	t.Stream = nil
	return true
}

type Registry struct {
	mu    sync.Mutex
	calls map[string]map[string]*Tracked
}

func NewRegistry() *Registry {
	return &Registry{calls: make(map[string]map[string]*Tracked)}
}

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

func (r *Registry) Get(channelID, callID string) (*Tracked, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	t, ok := r.calls[channelID][callID]
	return t, ok
}

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

func (r *Registry) Channels() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	channels := make([]string, 0, len(r.calls))
	for channelID := range r.calls {
		channels = append(channels, channelID)
	}
	return channels
}
