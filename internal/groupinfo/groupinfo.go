package groupinfo

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"go.mau.fi/whatsmeow/types"
)

const (
	defaultTTL                = 12 * time.Hour
	defaultRetryAfter         = 5 * time.Minute
	defaultIdleBeforeEviction = defaultTTL
	evictionThreshold         = 20000
)

type Lookup interface {
	GetGroupInfo(ctx context.Context, jid types.JID) (*types.GroupInfo, error)
}

type Options struct {
	TTL                time.Duration
	RetryAfter         time.Duration
	IdleBeforeEviction time.Duration
	Now                func() time.Time
}

type Cache struct {
	ttl                time.Duration
	retryAfter         time.Duration
	idleBeforeEviction time.Duration
	now                func() time.Time
	log                *slog.Logger

	mu      sync.Mutex
	entries map[string]*entry
}

type entry struct {
	mu        sync.Mutex
	name      string
	expiresAt time.Time
	loaded    bool
}

func New(opts Options, log *slog.Logger) *Cache {
	if opts.TTL <= 0 {
		opts.TTL = defaultTTL
	}
	if opts.RetryAfter <= 0 {
		opts.RetryAfter = defaultRetryAfter
	}
	if opts.IdleBeforeEviction <= 0 {
		opts.IdleBeforeEviction = opts.TTL
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Cache{
		ttl:                opts.TTL,
		retryAfter:         opts.RetryAfter,
		idleBeforeEviction: opts.IdleBeforeEviction,
		now:                opts.Now,
		log:                log,
		entries:            make(map[string]*entry),
	}
}

func (c *Cache) Name(ctx context.Context, lookup Lookup, channelID string, jid types.JID) string {
	if c == nil || lookup == nil {
		return ""
	}

	e := c.entryFor(channelID, jid)

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.loaded && c.now().Before(e.expiresAt) {
		return e.name
	}

	return c.resolve(ctx, e, lookup, channelID, jid)
}

func (c *Cache) resolve(ctx context.Context, e *entry, lookup Lookup, channelID string, jid types.JID) string {
	info, err := lookup.GetGroupInfo(ctx, jid)
	if err != nil {
		c.log.Warn("groupinfo: look up group info",
			"channel_id", channelID, "jid", jid.String(), "error", err)
		return e.store(e.name, c.now().Add(c.retryAfter))
	}

	return e.store(info.Name, c.now().Add(c.ttl))
}

func (e *entry) store(name string, expiresAt time.Time) string {
	e.name = name
	e.expiresAt = expiresAt
	e.loaded = true
	return name
}

func (c *Cache) Invalidate(channelID string, jid types.JID) {
	key := channelID + "|" + jid.String()

	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.entries, key)
}

func (c *Cache) entryFor(channelID string, jid types.JID) *entry {
	key := channelID + "|" + jid.String()

	c.mu.Lock()
	defer c.mu.Unlock()

	if e, ok := c.entries[key]; ok {
		return e
	}
	if len(c.entries) >= evictionThreshold {
		c.evictIdle()
	}
	e := &entry{}
	c.entries[key] = e
	return e
}

func (c *Cache) evictIdle() {
	cutoff := c.now().Add(-c.idleBeforeEviction)
	for key, e := range c.entries {
		if e.expiresAt.Before(cutoff) {
			delete(c.entries, key)
		}
	}
}
