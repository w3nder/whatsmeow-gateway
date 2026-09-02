package gateway

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/w3nder/whatsmeow-gateway/internal/avatar"
	"github.com/w3nder/whatsmeow-gateway/internal/call"
	"github.com/w3nder/whatsmeow-gateway/internal/session"
)

type identityClient struct {
	jid         *types.JID
	displayName string
	photoInfo   *types.ProfilePictureInfo
	photoErr    error
}

var _ session.WAClient = (*identityClient)(nil)

func (c *identityClient) QRChannel(context.Context) (<-chan whatsmeow.QRChannelItem, error) {
	return nil, nil
}
func (c *identityClient) Connect() error                       { return nil }
func (c *identityClient) IsLoggedIn() bool                     { return true }
func (c *identityClient) IsConnected() bool                    { return true }
func (c *identityClient) WaitForConnection(time.Duration) bool { return true }
func (c *identityClient) DeviceJID() *types.JID                { return c.jid }
func (c *identityClient) DisplayName() string                  { return c.displayName }
func (c *identityClient) SendMessage(context.Context, types.JID, *waE2E.Message, types.MessageID, []waBinary.Node) (whatsmeow.SendResponse, error) {
	return whatsmeow.SendResponse{}, nil
}
func (c *identityClient) BuildEdit(types.JID, types.MessageID, *waE2E.Message) *waE2E.Message {
	return nil
}
func (c *identityClient) BuildRevoke(types.JID, types.JID, types.MessageID) *waE2E.Message {
	return nil
}
func (c *identityClient) BuildReaction(types.JID, types.JID, types.MessageID, string) *waE2E.Message {
	return nil
}
func (c *identityClient) Upload(context.Context, []byte, whatsmeow.MediaType) (whatsmeow.UploadResponse, error) {
	return whatsmeow.UploadResponse{}, nil
}
func (c *identityClient) Download(context.Context, whatsmeow.DownloadableMessage) ([]byte, error) {
	return nil, nil
}
func (c *identityClient) PNForLID(context.Context, types.JID) (types.JID, bool, error) {
	return types.JID{}, false, nil
}
func (c *identityClient) DecryptSecretEncryptedMessage(context.Context, *events.Message) (*waE2E.Message, error) {
	return nil, nil
}
func (c *identityClient) DecryptPollVote(context.Context, *events.Message) (*waE2E.PollVoteMessage, error) {
	return nil, nil
}
func (c *identityClient) GetProfilePictureInfo(context.Context, types.JID, *whatsmeow.GetProfilePictureParams) (*types.ProfilePictureInfo, error) {
	return c.photoInfo, c.photoErr
}
func (c *identityClient) GetGroupInfo(context.Context, types.JID) (*types.GroupInfo, error) {
	return nil, nil
}
func (c *identityClient) AddEventHandler(func(any)) uint32 { return 0 }
func (c *identityClient) Calls() call.Caller               { return nil }
func (c *identityClient) Disconnect()                      {}

type identityStore struct {
	mu   sync.Mutex
	puts map[string][]byte
}

func (s *identityStore) Put(_ context.Context, key, _ string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.puts == nil {
		s.puts = make(map[string][]byte)
	}
	s.puts[key] = data
	return nil
}

func (s *identityStore) get(key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.puts[key]
	return data, ok
}

func newIdentityGateway(t *testing.T, store avatar.Store, fetch avatar.Fetch, client session.WAClient) *gateway {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := session.NewManager(func(string, *types.JID) (session.WAClient, error) {
		return client, nil
	})

	jid := client.DeviceJID()
	if jid == nil {
		empty := types.JID{}
		jid = &empty
	}
	if err := mgr.Resume(context.Background(), "channel-1", *jid); err != nil {
		t.Fatalf("Resume: %v", err)
	}

	return &gateway{
		manager:         mgr,
		avatars:         avatar.New(store, fetch, avatar.Options{}, logger),
		logger:          logger,
		tenantByChannel: make(map[string]string),
	}
}

func TestConnectedIdentityCarriesNamePhoneAndPhotoKey(t *testing.T) {
	jid := types.NewJID("5511999990000", types.DefaultUserServer)
	client := &identityClient{
		jid:         &jid,
		displayName: "Loja Vectax",
		photoInfo:   &types.ProfilePictureInfo{URL: "https://example.invalid/photo.jpg", ID: "pic-1"},
	}
	store := &identityStore{}
	g := newIdentityGateway(t, store, func(context.Context, string) ([]byte, error) {
		return []byte("photo-bytes"), nil
	}, client)

	phone, displayName, picture := g.connectedIdentity(context.Background(), "channel-1")

	if phone != "5511999990000" {
		t.Errorf("phone = %q, want 5511999990000", phone)
	}
	if displayName != "Loja Vectax" {
		t.Errorf("displayName = %q, want Loja Vectax", displayName)
	}
	if picture == nil || picture.Key == "" {
		t.Fatalf("picture = %+v, want a picture carrying a non-empty store key", picture)
	}
	if data, ok := store.get(picture.Key); !ok || string(data) != "photo-bytes" {
		t.Errorf("store did not receive the fetched photo under key %q", picture.Key)
	}
}

func TestConnectedIdentityWithNoPhotoStillCarriesNameAndPhone(t *testing.T) {
	jid := types.NewJID("5511988887777", types.DefaultUserServer)
	client := &identityClient{
		jid:         &jid,
		displayName: "Sem Foto Ltda",
		photoErr:    whatsmeow.ErrProfilePictureNotSet,
	}
	g := newIdentityGateway(t, &identityStore{}, func(context.Context, string) ([]byte, error) {
		t.Fatal("fetch should not be called when WhatsApp reports no photo")
		return nil, nil
	}, client)

	phone, displayName, picture := g.connectedIdentity(context.Background(), "channel-1")

	if phone != "5511988887777" {
		t.Errorf("phone = %q, want 5511988887777", phone)
	}
	if displayName != "Sem Foto Ltda" {
		t.Errorf("displayName = %q, want Sem Foto Ltda", displayName)
	}
	if picture != nil {
		t.Errorf("picture = %+v, want nil for an account with no photo", picture)
	}
}

func TestConnectedIdentityPhotoLookupFailureDoesNotBlock(t *testing.T) {
	jid := types.NewJID("5511977776666", types.DefaultUserServer)
	client := &identityClient{
		jid:         &jid,
		displayName: "Falha De Foto Ltda",
		photoErr:    errors.New("iq timed out"),
	}
	g := newIdentityGateway(t, &identityStore{}, func(context.Context, string) ([]byte, error) {
		t.Fatal("fetch should not be called when the picture IQ itself failed")
		return nil, nil
	}, client)

	start := time.Now()
	phone, displayName, picture := g.connectedIdentity(context.Background(), "channel-1")
	elapsed := time.Since(start)

	if phone != "5511977776666" || displayName != "Falha De Foto Ltda" {
		t.Errorf("phone/displayName = %q/%q, want the rest of the identity even when the photo lookup fails", phone, displayName)
	}
	if picture != nil {
		t.Errorf("picture = %+v, want nil when the photo lookup failed", picture)
	}
	if elapsed > time.Second {
		t.Errorf("connectedIdentity took %s for a lookup that fails immediately -- it must not add delay of its own", elapsed)
	}
}
