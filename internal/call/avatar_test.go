package call_test

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/avatar"
	"github.com/w3nder/whatsmeow-gateway/internal/call"
)

type fakeAvatars struct {
	mu   sync.Mutex
	pic  *avatar.Picture
	jids []types.JID
}

func (f *fakeAvatars) For(_ context.Context, jid types.JID) *avatar.Picture {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.jids = append(f.jids, jid)
	return f.pic
}

func (f *fakeAvatars) lookups() []types.JID {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]types.JID(nil), f.jids...)
}

func callPicture() *avatar.Picture {
	return &avatar.Picture{
		Key:      "profile-pictures/t1/5511888888888/pic-1",
		ID:       "pic-1",
		MimeType: "image/jpeg",
	}
}

func newAvatarTestManager(t *testing.T, pub call.Publisher, avatars call.AvatarSource) *call.Manager {
	t.Helper()
	return call.NewManager(pub, newMemStore(),
		func(channelID string) call.Identity {
			return call.Identity{PhoneNumberID: channelID, TenantID: "t1"}
		},
		nil,
		func(string) call.AvatarSource { return avatars },
		call.Options{TmpDir: t.TempDir(), Now: time.Now},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
}

func TestAnArrivingCallCarriesTheCallersProfilePicture(t *testing.T) {
	pub := &memPublisher{}
	avatars := &fakeAvatars{pic: callPicture()}
	m := newAvatarTestManager(t, pub, avatars)

	caller := &fakeCaller{}
	m.Attach("chan-a", caller)
	caller.fireIncoming(&fakeCall{id: "C1", peer: "5511888888888@s.whatsapp.net"})

	inbound := pub.inboundEvents()
	if len(inbound) != 1 {
		t.Fatalf("inbound events = %d, want 1", len(inbound))
	}
	if inbound[0].ProfilePicture == nil {
		t.Fatal("expected a profile picture on the inbound call event, got nil")
	}
	if *inbound[0].ProfilePicture != *callPicture() {
		t.Errorf("profile picture = %+v, want %+v", inbound[0].ProfilePicture, callPicture())
	}

	looked := avatars.lookups()
	if len(looked) != 1 {
		t.Fatalf("avatar lookups = %d, want 1", len(looked))
	}
	if looked[0].User != "5511888888888" {
		t.Errorf("looked up %s, want the peer 5511888888888", looked[0])
	}
}

func TestAnArrivingCallFromALIDPeerLooksUpThatIdentity(t *testing.T) {
	pub := &memPublisher{}
	avatars := &fakeAvatars{pic: callPicture()}
	m := newAvatarTestManager(t, pub, avatars)

	caller := &fakeCaller{}
	m.Attach("chan-a", caller)
	caller.fireIncoming(&fakeCall{id: "C1", peer: "173907587899617:14@lid"})

	looked := avatars.lookups()
	if len(looked) != 1 {
		t.Fatalf("avatar lookups = %d, want 1", len(looked))
	}
	if looked[0].User != "173907587899617" || looked[0].Server != types.HiddenUserServer {
		t.Errorf("looked up %s, want 173907587899617@lid", looked[0])
	}
}

func TestACallWePlacedLooksUpNoProfilePicture(t *testing.T) {
	pub := &memPublisher{}
	avatars := &fakeAvatars{pic: callPicture()}
	m := newAvatarTestManager(t, pub, avatars)

	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	err := m.Dispatch(context.Background(), caller, amqp.GatewayCallCommand{
		ChannelID: "chan-a", CallID: "C1", CommandID: "cmd-1", Action: "dial", To: "5511888888888",
	}, noFetch)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	inbound := pub.inboundEvents()
	if len(inbound) != 1 {
		t.Fatalf("inbound events = %d, want 1", len(inbound))
	}
	if inbound[0].ProfilePicture != nil {
		t.Errorf("profile picture = %+v, want nil on a call we placed", inbound[0].ProfilePicture)
	}
	if got := avatars.lookups(); len(got) != 0 {
		t.Errorf("avatar lookups = %d, want 0", len(got))
	}
}

func TestACallWithAnUnidentifiablePeerLooksUpNoProfilePicture(t *testing.T) {
	pub := &memPublisher{}
	avatars := &fakeAvatars{pic: callPicture()}
	m := newAvatarTestManager(t, pub, avatars)

	caller := &fakeCaller{}
	m.Attach("chan-a", caller)
	caller.fireIncoming(&fakeCall{id: "C1", peer: "not-a-jid"})

	inbound := pub.inboundEvents()
	if len(inbound) != 1 {
		t.Fatalf("inbound events = %d, want 1: an unreadable peer must not cost the call its event", len(inbound))
	}
	if inbound[0].SenderLid != "" || inbound[0].SenderPn != "" {
		t.Fatalf("peer resolved to %q/%q, so this test no longer covers an unidentifiable peer",
			inbound[0].SenderLid, inbound[0].SenderPn)
	}
	if got := avatars.lookups(); len(got) != 0 {
		t.Errorf("avatar lookups = %d, want 0", len(got))
	}
}

func TestACallOnAChannelWithNoAvatarSourceIsStillReported(t *testing.T) {
	pub := &memPublisher{}
	m := newAvatarTestManager(t, pub, nil)

	caller := &fakeCaller{}
	m.Attach("chan-a", caller)
	caller.fireIncoming(&fakeCall{id: "C1", peer: "5511888888888@s.whatsapp.net"})

	inbound := pub.inboundEvents()
	if len(inbound) != 1 {
		t.Fatalf("inbound events = %d, want 1", len(inbound))
	}
	if inbound[0].ProfilePicture != nil {
		t.Errorf("profile picture = %+v, want nil", inbound[0].ProfilePicture)
	}
}
