package call_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"

	"github.com/w3nder/whatsmeow-gateway/internal/call"
	"github.com/w3nder/whatsmeow-gateway/internal/senderid"
)

// fakeSenderResolver stands in for the channel's whatsmeow client's LID store
// lookup -- the same one an inbound message's sender resolution goes through.
type fakeSenderResolver struct {
	pn    types.JID
	found bool
}

func (f fakeSenderResolver) PNForLID(context.Context, types.JID) (types.JID, bool, error) {
	return f.pn, f.found, nil
}

// newIdentityTestManager builds a manager with a fixed sender resolver, for
// the tests below that only ever attach one channel.
func newIdentityTestManager(t *testing.T, pub call.Publisher, resolver senderid.Resolver) *call.Manager {
	t.Helper()
	return call.NewManager(pub, newMemStore(),
		func(channelID string) call.Identity {
			return call.Identity{PhoneNumberID: channelID, TenantID: "t1"}
		},
		func(string) senderid.Resolver { return resolver },
		call.Options{TmpDir: t.TempDir(), Now: time.Now},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
}

// assertSenderIdentity checks From/SenderLid/SenderPn together so a failure
// names which of the two events (call.Event or InboundCallEvent) disagreed
// with the expectation, and how.
func assertSenderIdentity(t *testing.T, label, from, senderLid, senderPn, wantFrom, wantLid, wantPn string) {
	t.Helper()
	if from != wantFrom {
		t.Errorf("%s.From = %q, want %q", label, from, wantFrom)
	}
	if senderLid != wantLid {
		t.Errorf("%s.SenderLid = %q, want %q", label, senderLid, wantLid)
	}
	if senderPn != wantPn {
		t.Errorf("%s.SenderPn = %q, want %q", label, senderPn, wantPn)
	}
}

// A call from a @lid peer whose phone number the LID store knows must carry
// the same senderLid/senderPn/from an inbound message from that same person
// would -- otherwise the backend's contact lookup, keyed on those strings,
// cannot find the contact a call just rang for. This is the exact shape of
// the real-call bug: peer "173907587899617:14@lid" resolving to phone number
// "5511988887777".
func TestManagerResolvesLIDPeerWithKnownPhoneNumber(t *testing.T) {
	pub := &memPublisher{}
	resolver := fakeSenderResolver{pn: types.NewJID("5511988887777", types.DefaultUserServer), found: true}
	m := newIdentityTestManager(t, pub, resolver)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	caller.fireIncoming(&fakeCall{id: "C1", peer: "173907587899617:14@lid"})

	incoming := pub.typed(call.EventIncoming)
	if len(incoming) != 1 {
		t.Fatalf("got %d incoming events, want 1", len(incoming))
	}
	assertSenderIdentity(t, "call.Event", incoming[0].From, incoming[0].SenderLid, incoming[0].SenderPn,
		"5511988887777", "173907587899617", "5511988887777")

	inbound := pub.inboundEvents()
	if len(inbound) != 1 {
		t.Fatalf("got %d inbound events, want 1", len(inbound))
	}
	assertSenderIdentity(t, "InboundCallEvent", inbound[0].From, inbound[0].SenderLid, inbound[0].SenderPn,
		"5511988887777", "173907587899617", "5511988887777")
}

// A @lid peer the LID store has no mapping for falls back to the lid itself
// as from, with senderPn left empty -- the fallback that keeps a call
// resolvable even when the store lookup comes up empty.
func TestManagerResolvesLIDPeerWithNoKnownPhoneNumber(t *testing.T) {
	pub := &memPublisher{}
	m := newIdentityTestManager(t, pub, fakeSenderResolver{found: false})
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	caller.fireIncoming(&fakeCall{id: "C1", peer: "173907587899617:14@lid"})

	incoming := pub.typed(call.EventIncoming)
	if len(incoming) != 1 {
		t.Fatalf("got %d incoming events, want 1", len(incoming))
	}
	assertSenderIdentity(t, "call.Event", incoming[0].From, incoming[0].SenderLid, incoming[0].SenderPn,
		"173907587899617", "173907587899617", "")

	inbound := pub.inboundEvents()
	if len(inbound) != 1 {
		t.Fatalf("got %d inbound events, want 1", len(inbound))
	}
	assertSenderIdentity(t, "InboundCallEvent", inbound[0].From, inbound[0].SenderLid, inbound[0].SenderPn,
		"173907587899617", "173907587899617", "")
}

// A peer that is already a phone JID needs no resolution: from is the phone
// number and senderLid stays empty, matching a message from a phone-JID
// sender with no lid alt.
func TestManagerPeerAlreadyPhoneJIDNeedsNoResolution(t *testing.T) {
	pub := &memPublisher{}
	m := newIdentityTestManager(t, pub, fakeSenderResolver{found: false})
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	caller.fireIncoming(&fakeCall{id: "C1", peer: "5511888888888@s.whatsapp.net"})

	incoming := pub.typed(call.EventIncoming)
	if len(incoming) != 1 {
		t.Fatalf("got %d incoming events, want 1", len(incoming))
	}
	assertSenderIdentity(t, "call.Event", incoming[0].From, incoming[0].SenderLid, incoming[0].SenderPn,
		"5511888888888", "", "5511888888888")

	inbound := pub.inboundEvents()
	if len(inbound) != 1 {
		t.Fatalf("got %d inbound events, want 1", len(inbound))
	}
	assertSenderIdentity(t, "InboundCallEvent", inbound[0].From, inbound[0].SenderLid, inbound[0].SenderPn,
		"5511888888888", "", "5511888888888")
}
