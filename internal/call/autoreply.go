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

func (c *Cooldown) Allow(channelID, caller string) bool {
	key := channelID + "|" + caller
	now := c.now()

	c.mu.Lock()
	defer c.mu.Unlock()

	for k, at := range c.last {
		if now.Sub(at) >= c.window {
			delete(c.last, k)
		}
	}
	if _, cooling := c.last[key]; cooling {
		return false
	}
	c.last[key] = now
	return true
}

func (m *Manager) autoReply(channelID string, lc LiveCall, message, senderLid, senderPn string) {
	if message == "" || m.opts.Replier == nil || lc.IsGroup() {
		return
	}

	caller := senderLid
	if caller == "" {
		caller = senderPn
	}
	if caller == "" {
		m.log.Warn("call: auto reply skipped, the caller has no known identity",
			"channel_id", channelID, "call_id", lc.ID())
		return
	}

	if !m.cooldown.Allow(channelID, caller) {
		m.log.Info("call: auto reply skipped, the caller was answered recently",
			"channel_id", channelID, "call_id", lc.ID())
		return
	}

	reply := AutoReply{ChannelID: channelID, CallID: lc.ID(), SenderLid: senderLid, SenderPn: senderPn, Text: message}

	m.replyWG.Add(1)
	go func() {
		defer m.replyWG.Done()
		if err := m.opts.Replier.Reply(context.Background(), reply); err != nil {
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
