// Package avatar resolves a WhatsApp contact's profile photo into a
// media-store key, once, and remembers the answer.
//
// Both event kinds that name a contact want the same thing -- an inbound
// message and an inbound call each create the contact's chat row on the
// backend, and each should carry their photo -- but internal/mapper and
// internal/call must not depend on each other, for the same reason
// internal/senderid exists. Both depend on this package instead.
//
// The cache is what makes the feature affordable. Without it every inbound
// message would cost an IQ round trip to WhatsApp, an HTTPS download and an
// object-store upload; with it, a busy contact costs nothing at all, and a
// stale entry usually costs only the IQ -- whatsmeow answers a recheck that
// carries the known picture id with no info and no error, meaning "not
// changed", and the photo is neither downloaded nor uploaded again.
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
	// ttl is how long a known photo is trusted before it is rechecked. A
	// profile photo changes rarely, and a recheck that finds it unchanged is
	// cheap, so this is generous on purpose.
	ttl = 24 * time.Hour
	// negativeTTL is how long "this contact has no photo we can see" is
	// trusted. Shorter than ttl: the answer flips the moment the contact sets
	// a photo or stops hiding it from us, and nothing tells us when.
	negativeTTL = 6 * time.Hour
	// retryAfter is how long a failed lookup is left alone. It exists to keep
	// a WhatsApp-side outage from turning every inbound message into another
	// doomed IQ.
	retryAfter = 5 * time.Minute
	// lookupTimeout bounds one resolution -- IQ, download and upload
	// together. This runs on the path that publishes an event, so a photo
	// that will not come quickly is not worth waiting for.
	lookupTimeout = 5 * time.Second
	// idleBeforeEviction is how long past its expiry an entry must sit before
	// a sweep may drop it. Well beyond ttl, so only contacts nobody has
	// spoken to in days are evicted: dropping a live entry would throw away
	// the picture id that lets its next recheck skip the download.
	idleBeforeEviction = ttl
	// evictionThreshold is the entry count that triggers that sweep. The
	// cache is otherwise unbounded, and a long-lived multi-tenant gateway
	// meets a lot of contacts.
	evictionThreshold = 20000

	// mimeType is what WhatsApp serves profile photos as. The IQ response
	// describes no other content type, so there is nothing to detect.
	mimeType = "image/jpeg"
)

// Picture is a contact's profile photo as an inbound event carries it: the
// key it was stored under, never the WhatsApp URL it came from. That URL is
// signed and short-lived, so an event holding one would be worthless by the
// time anything acted on it.
//
// Key embeds ID, so a contact who changes their photo produces a new key --
// the backend sees the change without comparing anything -- while a contact
// who does not always produces the same key, making a repeated upload
// idempotent rather than a new object.
type Picture struct {
	Key      string `json:"key"`
	ID       string `json:"id,omitempty"`
	MimeType string `json:"mimeType,omitempty"`
}

// Lookup is the single whatsmeow query this package needs, which a channel's
// own client satisfies. Profile-photo visibility is a privacy setting
// evaluated against the account doing the asking, which is why the caller
// passes the channel's client rather than this package holding one.
type Lookup interface {
	GetProfilePictureInfo(ctx context.Context, jid types.JID, params *whatsmeow.GetProfilePictureParams) (*types.ProfilePictureInfo, error)
}

// Store is where the photo lands -- the gateway's media store, the same one
// inbound message media goes to.
type Store interface {
	Put(ctx context.Context, key, mime string, data []byte) error
}

// Fetch downloads the photo from the URL WhatsApp handed back.
type Fetch func(ctx context.Context, url string) ([]byte, error)

type Options struct {
	// Now is injected by tests. Nil means time.Now.
	Now func() time.Time
}

// Cache resolves photos and remembers them, per channel and contact.
type Cache struct {
	store Store
	fetch Fetch
	now   func() time.Time
	log   *slog.Logger

	// mu guards the entries map only. Each entry carries its own lock, held
	// across the network round trip, so one slow contact never blocks another
	// -- and so a burst of messages from the same contact resolves once
	// rather than once per message.
	mu      sync.Mutex
	entries map[string]*entry
}

type entry struct {
	mu        sync.Mutex
	pic       *Picture
	expiresAt time.Time
	// loaded distinguishes "never resolved" from "resolved to no photo",
	// which are both a nil pic.
	loaded bool
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

// For returns jid's profile photo as seen by channelID, resolving and storing
// it when nothing fresh is cached.
//
// It reports no error, by design. A photo is decoration on an event that
// carries a message or a call, and neither may be dropped, delayed past a
// short timeout, or failed because WhatsApp would not answer a photo query.
// Every failure is logged and comes back as nil, and the event simply goes
// out without a picture.
func (c *Cache) For(ctx context.Context, lookup Lookup, channelID, tenantID string, jid types.JID) *Picture {
	if c == nil || lookup == nil {
		return nil
	}

	// A sender JID names the phone a message came from; a profile photo
	// belongs to the account behind it. Dropping the device suffix here, once,
	// keeps callers from having to know that -- and keeps one contact from
	// being cached and stored once per phone they answer on.
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

// resolve refreshes e from WhatsApp. e.mu is held.
func (c *Cache) resolve(ctx context.Context, e *entry, lookup Lookup, channelID, tenantID string, jid types.JID) *Picture {
	var existingID string
	if e.pic != nil {
		existingID = e.pic.ID
	}

	info, err := lookup.GetProfilePictureInfo(ctx, jid, &whatsmeow.GetProfilePictureParams{ExistingID: existingID})
	switch {
	case errors.Is(err, whatsmeow.ErrProfilePictureNotSet), errors.Is(err, whatsmeow.ErrProfilePictureUnauthorized):
		// Both are settled answers rather than failures: the contact has no
		// photo, or has hidden it from this channel. Remember it, so a chatty
		// contact without a photo does not cost an IQ per message.
		return e.store(nil, c.now().Add(negativeTTL))

	case err != nil:
		c.log.Warn("avatar: look up profile picture",
			"channel_id", channelID, "jid", jid.String(), "error", err)
		// Keep serving whatever was last known -- a photo we already stored
		// does not stop being that contact's photo because an IQ timed out.
		return e.store(e.pic, c.now().Add(retryAfter))

	case info == nil, info.URL == "":
		// whatsmeow answers a recheck whose ExistingID still matches with no
		// info and no error: the photo has not changed, so the stored object
		// is still current and nothing needs downloading.
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

// ttlFor keeps an unresolved-to-nothing entry on the shorter negative TTL: an
// answer that yielded no picture is not worth trusting for a whole day.
func (c *Cache) ttlFor(pic *Picture) time.Duration {
	if pic == nil {
		return negativeTTL
	}
	return ttl
}

// store records the outcome on e and hands it back, so every branch of
// resolve is one line. e.mu is held.
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

// evictIdle drops entries that expired long enough ago that nobody is talking
// to those contacts any more. c.mu is held.
//
// This is deliberately invisible from the outside: an evicted contact's next
// event resolves them from scratch, which is what an expired entry does
// anyway. Only the picture id hint is lost, and only for contacts that have
// been silent for over a day.
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
