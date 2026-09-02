package avatar

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

func extensionFor(mimeType string) string {
	switch mimeType {
	case "image/png":
		return ".png"
	case "image/webp":
		return ".webp"
	default:
		return ".jpg"
	}
}

const (
	ttl                = 24 * time.Hour
	negativeTTL        = 6 * time.Hour
	retryAfter         = 5 * time.Minute
	lookupTimeout      = 5 * time.Second
	idleBeforeEviction = ttl
	evictionThreshold  = 20000

	mimeType = "image/jpeg"
)

type Picture struct {
	Key      string `json:"key"`
	ID       string `json:"id,omitempty"`
	MimeType string `json:"mimeType,omitempty"`
}

type Lookup interface {
	GetProfilePictureInfo(ctx context.Context, jid types.JID, params *whatsmeow.GetProfilePictureParams) (*types.ProfilePictureInfo, error)
}

type Store interface {
	Put(ctx context.Context, key, mime string, data []byte) error
}

type Fetch func(ctx context.Context, url string) ([]byte, error)

type Options struct {
	Now func() time.Time
}

type Cache struct {
	store Store
	fetch Fetch
	now   func() time.Time
	log   *slog.Logger

	mu      sync.Mutex
	entries map[string]*entry
}

type entry struct {
	mu        sync.Mutex
	pic       *Picture
	expiresAt time.Time
	loaded    bool
}

func New(store Store, fetch Fetch, opts Options, log *slog.Logger) *Cache {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Cache{
		store:   store,
		fetch:   fetch,
		now:     opts.Now,
		log:     log,
		entries: make(map[string]*entry),
	}
}

func (c *Cache) For(ctx context.Context, lookup Lookup, channelID, tenantID string, jid types.JID) *Picture {
	if c == nil || lookup == nil {
		return nil
	}

	jid = jid.ToNonAD()

	e := c.entryFor(channelID, jid)

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.loaded && c.now().Before(e.expiresAt) {
		return e.pic
	}

	ctx, cancel := context.WithTimeout(ctx, lookupTimeout)
	defer cancel()

	return c.resolve(ctx, e, lookup, channelID, tenantID, jid)
}

func (c *Cache) resolve(ctx context.Context, e *entry, lookup Lookup, channelID, tenantID string, jid types.JID) *Picture {
	var existingID string
	if e.pic != nil {
		existingID = e.pic.ID
	}

	info, err := lookup.GetProfilePictureInfo(ctx, jid, &whatsmeow.GetProfilePictureParams{ExistingID: existingID})
	switch {
	case errors.Is(err, whatsmeow.ErrProfilePictureNotSet), errors.Is(err, whatsmeow.ErrProfilePictureUnauthorized):
		return e.store(nil, c.now().Add(negativeTTL))

	case err != nil:
		c.log.Warn("avatar: look up profile picture",
			"channel_id", channelID, "jid", jid.String(), "error", err)
		return e.store(e.pic, c.now().Add(retryAfter))

	case info == nil, info.URL == "":
		return e.store(e.pic, c.now().Add(c.ttlFor(e.pic)))
	}

	data, err := c.fetch(ctx, info.URL)
	if err != nil {
		c.log.Warn("avatar: download profile picture",
			"channel_id", channelID, "jid", jid.String(), "picture_id", info.ID, "error", err)
		return e.store(e.pic, c.now().Add(retryAfter))
	}

	key := fmt.Sprintf("profile-pictures/%s%s", uuid.NewString(), extensionFor(mimeType))
	if err := c.store.Put(ctx, key, mimeType, data); err != nil {
		c.log.Warn("avatar: store profile picture",
			"channel_id", channelID, "jid", jid.String(), "key", key, "error", err)
		return e.store(e.pic, c.now().Add(retryAfter))
	}

	return e.store(&Picture{Key: key, ID: info.ID, MimeType: mimeType}, c.now().Add(ttl))
}

func (c *Cache) ttlFor(pic *Picture) time.Duration {
	if pic == nil {
		return negativeTTL
	}
	return ttl
}

func (e *entry) store(pic *Picture, expiresAt time.Time) *Picture {
	e.pic = pic
	e.expiresAt = expiresAt
	e.loaded = true
	return pic
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
	cutoff := c.now().Add(-idleBeforeEviction)
	for key, e := range c.entries {
		if !e.mu.TryLock() {
			continue
		}
		expired := e.expiresAt.Before(cutoff)
		e.mu.Unlock()
		if expired {
			delete(c.entries, key)
		}
	}
}
