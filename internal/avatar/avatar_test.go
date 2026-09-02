package avatar_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"

	"github.com/w3nder/whatsmeow-gateway/internal/avatar"
)

const (
	tenantID  = "tenant-1"
	channelID = "channel-1"
)

func peerJID() types.JID {
	return types.NewJID("5511999999999", types.DefaultUserServer)
}

type lookupCall struct {
	jid    types.JID
	params *whatsmeow.GetProfilePictureParams
}

type stubLookup struct {
	mu    sync.Mutex
	calls []lookupCall
	fn    func(n int, params *whatsmeow.GetProfilePictureParams) (*types.ProfilePictureInfo, error)
}

func (s *stubLookup) GetProfilePictureInfo(ctx context.Context, jid types.JID, params *whatsmeow.GetProfilePictureParams) (*types.ProfilePictureInfo, error) {
	s.mu.Lock()
	n := len(s.calls)
	s.calls = append(s.calls, lookupCall{jid: jid, params: params})
	s.mu.Unlock()
	return s.fn(n, params)
}

func (s *stubLookup) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

func (s *stubLookup) call(i int) lookupCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[i]
}

func always(info *types.ProfilePictureInfo, err error) func(int, *whatsmeow.GetProfilePictureParams) (*types.ProfilePictureInfo, error) {
	return func(int, *whatsmeow.GetProfilePictureParams) (*types.ProfilePictureInfo, error) {
		return info, err
	}
}

type put struct {
	key  string
	mime string
	data []byte
}

type stubStore struct {
	mu   sync.Mutex
	puts []put
	err  error
}

func (s *stubStore) Put(ctx context.Context, key, mime string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.puts = append(s.puts, put{key: key, mime: mime, data: data})
	return nil
}

func (s *stubStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.puts)
}

func (s *stubStore) last() put {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.puts[len(s.puts)-1]
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock {
	return &clock{t: time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)}
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func fetchBytes(data []byte) avatar.Fetch {
	return func(context.Context, string) ([]byte, error) { return data, nil }
}

func newCache(t *testing.T, store avatar.Store, fetch avatar.Fetch, c *clock) *avatar.Cache {
	t.Helper()
	return avatar.New(store, fetch, avatar.Options{Now: c.now}, quietLogger())
}

func TestForStoresPhotoUnderTenantAndPictureID(t *testing.T) {
	lookup := &stubLookup{fn: always(&types.ProfilePictureInfo{URL: "https://pps.example/photo", ID: "pic-1"}, nil)}
	store := &stubStore{}
	cache := newCache(t, store, fetchBytes([]byte("jpeg-bytes")), newClock())

	pic := cache.For(context.Background(), lookup, channelID, tenantID, peerJID())

	if pic == nil {
		t.Fatal("expected a picture, got nil")
	}
	if strings.Contains(pic.Key, tenantID) || strings.Contains(pic.Key, "5511999999999") || strings.Contains(pic.Key, "pic-1") {
		t.Errorf("key %q leaks tenant, phone or picture id; a public object must not be guessable", pic.Key)
	}
	if !strings.HasPrefix(pic.Key, "profile-pictures/") || !strings.HasSuffix(pic.Key, ".jpg") {
		t.Errorf("key = %q, want profile-pictures/<random>.jpg", pic.Key)
	}
	if pic.ID != "pic-1" {
		t.Errorf("id = %q, want %q", pic.ID, "pic-1")
	}
	if pic.MimeType != "image/jpeg" {
		t.Errorf("mimeType = %q, want %q", pic.MimeType, "image/jpeg")
	}

	if store.count() != 1 {
		t.Fatalf("store puts = %d, want 1", store.count())
	}
	stored := store.last()
	if stored.key != pic.Key {
		t.Errorf("stored key = %q, want the key the cache reported (%q)", stored.key, pic.Key)
	}
	if stored.mime != "image/jpeg" {
		t.Errorf("stored mime = %q, want image/jpeg", stored.mime)
	}
	if string(stored.data) != "jpeg-bytes" {
		t.Errorf("stored data = %q, want %q", stored.data, "jpeg-bytes")
	}
}

func TestForAsksAboutThePersonRatherThanTheDeviceThatMessaged(t *testing.T) {
	lookup := &stubLookup{fn: always(&types.ProfilePictureInfo{URL: "https://pps.example/photo", ID: "pic-1"}, nil)}
	store := &stubStore{}
	cache := newCache(t, store, fetchBytes([]byte("jpeg-bytes")), newClock())

	fromPhone := peerJID()
	fromPhone.Device = 14

	pic := cache.For(context.Background(), lookup, channelID, tenantID, fromPhone)

	if got := lookup.call(0).jid.Device; got != 0 {
		t.Errorf("queried device %d, want the bare user", got)
	}
	if pic == nil || !strings.HasPrefix(pic.Key, "profile-pictures/") {
		t.Errorf("picture = %+v, want the device-free key", pic)
	}
}

func TestForServesCachedPictureWithoutQueryingWhatsApp(t *testing.T) {
	lookup := &stubLookup{fn: always(&types.ProfilePictureInfo{URL: "https://pps.example/photo", ID: "pic-1"}, nil)}
	store := &stubStore{}
	cache := newCache(t, store, fetchBytes([]byte("jpeg-bytes")), newClock())

	first := cache.For(context.Background(), lookup, channelID, tenantID, peerJID())
	second := cache.For(context.Background(), lookup, channelID, tenantID, peerJID())

	if lookup.count() != 1 {
		t.Errorf("lookups = %d, want 1", lookup.count())
	}
	if store.count() != 1 {
		t.Errorf("store puts = %d, want 1", store.count())
	}
	if second == nil || first == nil || *second != *first {
		t.Errorf("second = %+v, want same picture as first %+v", second, first)
	}
}

func TestForRechecksWithExistingIDOnceTheEntryIsStale(t *testing.T) {
	lookup := &stubLookup{fn: func(n int, _ *whatsmeow.GetProfilePictureParams) (*types.ProfilePictureInfo, error) {
		if n == 0 {
			return &types.ProfilePictureInfo{URL: "https://pps.example/photo", ID: "pic-1"}, nil
		}
		return nil, nil
	}}
	store := &stubStore{}
	c := newClock()
	cache := newCache(t, store, fetchBytes([]byte("jpeg-bytes")), c)

	first := cache.For(context.Background(), lookup, channelID, tenantID, peerJID())
	c.advance(25 * time.Hour)
	second := cache.For(context.Background(), lookup, channelID, tenantID, peerJID())

	if lookup.count() != 2 {
		t.Fatalf("lookups = %d, want 2", lookup.count())
	}
	if got := lookup.call(1).params.ExistingID; got != "pic-1" {
		t.Errorf("recheck ExistingID = %q, want %q", got, "pic-1")
	}
	if store.count() != 1 {
		t.Errorf("store puts = %d, want 1: an unchanged photo must not be downloaded again", store.count())
	}
	if second == nil || *second != *first {
		t.Errorf("second = %+v, want same picture as first %+v", second, first)
	}
}

func TestForStoresTheNewPhotoWhenThePictureChanged(t *testing.T) {
	lookup := &stubLookup{fn: func(n int, _ *whatsmeow.GetProfilePictureParams) (*types.ProfilePictureInfo, error) {
		if n == 0 {
			return &types.ProfilePictureInfo{URL: "https://pps.example/old", ID: "pic-1"}, nil
		}
		return &types.ProfilePictureInfo{URL: "https://pps.example/new", ID: "pic-2"}, nil
	}}
	store := &stubStore{}
	c := newClock()
	cache := newCache(t, store, fetchBytes([]byte("jpeg-bytes")), c)

	cache.For(context.Background(), lookup, channelID, tenantID, peerJID())
	c.advance(25 * time.Hour)
	second := cache.For(context.Background(), lookup, channelID, tenantID, peerJID())

	if store.count() != 2 {
		t.Fatalf("store puts = %d, want 2", store.count())
	}
	if second == nil || !strings.HasPrefix(second.Key, "profile-pictures/") {
		t.Errorf("second key = %+v, want a profile-pictures key", second)
	}
	if store.last().key != second.Key {
		t.Errorf("stored key = %q, want the key the cache reported (%q)", store.last().key, second.Key)
	}
}

func TestForRemembersThatAContactHasNoReachablePhoto(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "not set", err: whatsmeow.ErrProfilePictureNotSet},
		{name: "hidden from us", err: whatsmeow.ErrProfilePictureUnauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lookup := &stubLookup{fn: always(nil, tc.err)}
			store := &stubStore{}
			cache := newCache(t, store, fetchBytes([]byte("jpeg-bytes")), newClock())

			if pic := cache.For(context.Background(), lookup, channelID, tenantID, peerJID()); pic != nil {
				t.Errorf("picture = %+v, want nil", pic)
			}
			if pic := cache.For(context.Background(), lookup, channelID, tenantID, peerJID()); pic != nil {
				t.Errorf("picture = %+v, want nil", pic)
			}

			if lookup.count() != 1 {
				t.Errorf("lookups = %d, want 1: a contact with no photo must not be asked about again", lookup.count())
			}
			if store.count() != 0 {
				t.Errorf("store puts = %d, want 0", store.count())
			}
		})
	}
}

func TestForServesTheKnownPhotoWhenTheRecheckFails(t *testing.T) {
	lookup := &stubLookup{fn: func(n int, _ *whatsmeow.GetProfilePictureParams) (*types.ProfilePictureInfo, error) {
		if n == 0 {
			return &types.ProfilePictureInfo{URL: "https://pps.example/photo", ID: "pic-1"}, nil
		}
		return nil, errors.New("iq timed out")
	}}
	store := &stubStore{}
	c := newClock()
	cache := newCache(t, store, fetchBytes([]byte("jpeg-bytes")), c)

	first := cache.For(context.Background(), lookup, channelID, tenantID, peerJID())
	c.advance(25 * time.Hour)
	second := cache.For(context.Background(), lookup, channelID, tenantID, peerJID())

	if second == nil || *second != *first {
		t.Errorf("second = %+v, want the last known picture %+v", second, first)
	}
}

func TestForDoesNotRetryAFailedLookupUntilTheRetryWindowPasses(t *testing.T) {
	lookup := &stubLookup{fn: always(nil, errors.New("iq timed out"))}
	c := newClock()
	cache := newCache(t, &stubStore{}, fetchBytes([]byte("jpeg-bytes")), c)

	cache.For(context.Background(), lookup, channelID, tenantID, peerJID())
	cache.For(context.Background(), lookup, channelID, tenantID, peerJID())
	if lookup.count() != 1 {
		t.Fatalf("lookups = %d, want 1 within the retry window", lookup.count())
	}

	c.advance(6 * time.Minute)
	cache.For(context.Background(), lookup, channelID, tenantID, peerJID())
	if lookup.count() != 2 {
		t.Errorf("lookups = %d, want 2 once the retry window passed", lookup.count())
	}
}

func TestForReturnsNothingWhenTheDownloadFails(t *testing.T) {
	lookup := &stubLookup{fn: always(&types.ProfilePictureInfo{URL: "https://pps.example/photo", ID: "pic-1"}, nil)}
	store := &stubStore{}
	fetch := avatar.Fetch(func(context.Context, string) ([]byte, error) {
		return nil, errors.New("connection reset")
	})
	cache := newCache(t, store, fetch, newClock())

	if pic := cache.For(context.Background(), lookup, channelID, tenantID, peerJID()); pic != nil {
		t.Errorf("picture = %+v, want nil", pic)
	}
	if store.count() != 0 {
		t.Errorf("store puts = %d, want 0", store.count())
	}
}

func TestForReturnsNothingWhenTheUploadFails(t *testing.T) {
	lookup := &stubLookup{fn: always(&types.ProfilePictureInfo{URL: "https://pps.example/photo", ID: "pic-1"}, nil)}
	store := &stubStore{err: errors.New("s3 unavailable")}
	cache := newCache(t, store, fetchBytes([]byte("jpeg-bytes")), newClock())

	if pic := cache.For(context.Background(), lookup, channelID, tenantID, peerJID()); pic != nil {
		t.Errorf("picture = %+v, want nil", pic)
	}
}

func TestForKeepsChannelsApart(t *testing.T) {
	lookup := &stubLookup{fn: always(&types.ProfilePictureInfo{URL: "https://pps.example/photo", ID: "pic-1"}, nil)}
	cache := newCache(t, &stubStore{}, fetchBytes([]byte("jpeg-bytes")), newClock())

	cache.For(context.Background(), lookup, "channel-a", tenantID, peerJID())
	cache.For(context.Background(), lookup, "channel-b", tenantID, peerJID())

	if lookup.count() != 2 {
		t.Errorf("lookups = %d, want 2: each channel asks for itself", lookup.count())
	}
}

func TestForQueriesOnceWhenEventsForOneContactArriveTogether(t *testing.T) {
	release := make(chan struct{})
	lookup := &stubLookup{fn: func(int, *whatsmeow.GetProfilePictureParams) (*types.ProfilePictureInfo, error) {
		<-release
		return &types.ProfilePictureInfo{URL: "https://pps.example/photo", ID: "pic-1"}, nil
	}}
	store := &stubStore{}
	cache := newCache(t, store, fetchBytes([]byte("jpeg-bytes")), newClock())

	const callers = 8
	var wg sync.WaitGroup
	pics := make([]*avatar.Picture, callers)
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pics[i] = cache.For(context.Background(), lookup, channelID, tenantID, peerJID())
		}()
	}

	close(release)
	wg.Wait()

	if lookup.count() != 1 {
		t.Errorf("lookups = %d, want 1", lookup.count())
	}
	if store.count() != 1 {
		t.Errorf("store puts = %d, want 1", store.count())
	}
	for i, pic := range pics {
		if pic == nil {
			t.Fatalf("caller %d got nil", i)
		}
	}
}

func TestForWithoutALookupReturnsNothing(t *testing.T) {
	cache := newCache(t, &stubStore{}, fetchBytes([]byte("jpeg-bytes")), newClock())

	if pic := cache.For(context.Background(), nil, channelID, tenantID, peerJID()); pic != nil {
		t.Errorf("picture = %+v, want nil", pic)
	}
}
