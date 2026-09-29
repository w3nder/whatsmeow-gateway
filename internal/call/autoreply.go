package call

import (
	"context"
	"sync"
	"time"
)

const CallRejectMessageCooldown = 10 * time.Minute

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

	mu   sync.Mutex
	last map[string]time.Time
}

func NewCooldown(window time.Duration, now func() time.Time) *Cooldown {
	return &Cooldown{window: window, now: now, last: make(map[string]time.Time)}
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

func (c *Cooldown) Allow(channelID string, callers ...string) bool {
	keys := cooldownKeys(channelID, callers)
	if len(keys) == 0 {
		return false
	}
	now := c.now()

	c.mu.Lock()
	defer c.mu.Unlock()

	for k, at := range c.last {
		if now.Sub(at) >= c.window {
			delete(c.last, k)
		}
	}
	for _, key := range keys {
		if _, cooling := c.last[key]; cooling {
			return false
		}
	}
	for _, key := range keys {
		c.last[key] = now
	}
	return true
}

func (c *Cooldown) Release(channelID string, callers ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, key := range cooldownKeys(channelID, callers) {
		delete(c.last, key)
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

	if !m.cooldown.Allow(channelID, senderLid, senderPn) {
		m.log.Info("call: auto reply skipped, the caller was answered recently",
			"channel_id", channelID, "call_id", lc.ID())
		return
	}

	reply := AutoReply{ChannelID: channelID, CallID: lc.ID(), SenderLid: senderLid, SenderPn: senderPn, Text: message}

	m.replyWG.Add(1)
	go func() {
		defer m.replyWG.Done()
		if err := m.opts.Replier.Reply(context.Background(), reply); err != nil {
			m.cooldown.Release(channelID, senderLid, senderPn)
			m.log.Error("call: auto reply failed",
				"channel_id", channelID, "call_id", reply.CallID, "error", err)
		}
	}()
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
