package call

import (
	"context"
	"sync"
	"time"
)

const (
	CallRejectMessageCooldown = 10 * time.Minute
	CallRejectSendTimeout     = 30 * time.Second
)

type AutoReply struct {
	ChannelID string
	CallID    string
	SenderLid string
	SenderPn  string
	Text      string
}

type AutoReplier interface {
	Reply(ctx context.Context, reply AutoReply) error
}

type Cooldown struct {
	window time.Duration
	now    func() time.Time

	mu      sync.Mutex
	last    map[string]reservation
	counter uint64
}

type reservation struct {
	at    time.Time
	token uint64
}

type Reservation struct {
	keys  []string
	token uint64
}

func NewCooldown(window time.Duration, now func() time.Time) *Cooldown {
	return &Cooldown{window: window, now: now, last: make(map[string]reservation)}
}

func cooldownKeys(channelID string, callers []string) []string {
	keys := make([]string, 0, len(callers))
	for _, caller := range callers {
		if caller != "" {
			keys = append(keys, channelID+"|"+caller)
		}
	}
	return keys
}

func (c *Cooldown) Allow(channelID string, callers ...string) (Reservation, bool) {
	keys := cooldownKeys(channelID, callers)
	if len(keys) == 0 {
		return Reservation{}, false
	}
	now := c.now()

	c.mu.Lock()
	defer c.mu.Unlock()

	for k, held := range c.last {
		if now.Sub(held.at) >= c.window {
			delete(c.last, k)
		}
	}
	for _, key := range keys {
		if _, cooling := c.last[key]; cooling {
			return Reservation{}, false
		}
	}
	c.counter++
	for _, key := range keys {
		c.last[key] = reservation{at: now, token: c.counter}
	}
	return Reservation{keys: keys, token: c.counter}, true
}

func (c *Cooldown) Release(held Reservation) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, key := range held.keys {
		if current, ok := c.last[key]; ok && current.token == held.token {
			delete(c.last, key)
		}
	}
}

func (m *Manager) autoReply(channelID string, lc LiveCall, message, senderLid, senderPn string) {
	if message == "" || m.opts.Replier == nil || lc.IsGroup() {
		return
	}

	if senderLid == "" && senderPn == "" {
		m.log.Warn("call: auto reply skipped, the caller has no known identity",
			"channel_id", channelID, "call_id", lc.ID())
		return
	}

	held, allowed := m.cooldown.Allow(channelID, senderLid, senderPn)
	if !allowed {
		m.log.Info("call: auto reply skipped, the caller was answered recently",
			"channel_id", channelID, "call_id", lc.ID())
		return
	}

	reply := AutoReply{ChannelID: channelID, CallID: lc.ID(), SenderLid: senderLid, SenderPn: senderPn, Text: message}

	m.replyWG.Add(1)
	go func() {
		defer m.replyWG.Done()
		ctx, cancel := context.WithTimeout(context.Background(), m.replyTimeout())
		defer cancel()
		if err := m.opts.Replier.Reply(ctx, reply); err != nil {
			m.cooldown.Release(held)
			m.log.Error("call: auto reply failed",
				"channel_id", channelID, "call_id", reply.CallID, "error", err)
		}
	}()
}

func (m *Manager) replyTimeout() time.Duration {
	if m.opts.ReplyTimeout > 0 {
		return m.opts.ReplyTimeout
	}
	return CallRejectSendTimeout
}

func (m *Manager) WaitForReplies(timeout time.Duration) {
	if !waitGroupWithin(&m.replyWG, timeout) {
		m.log.Error("call: shutdown timed out waiting for auto replies", "timeout", timeout)
	}
}

func waitGroupWithin(wg *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}
