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

type fakeSenderResolver struct {
	pn    types.JID
	found bool
}

func (f fakeSenderResolver) PNForLID(context.Context, types.JID) (types.JID, bool, error) {
	return f.pn, f.found, nil
}

func newIdentityTestManager(t *testing.T, pub call.Publisher, resolver senderid.Resolver) *call.Manager {
	t.Helper()
	return call.NewManager(pub, newMemStore(),
		func(channelID string) call.Identity {
			return call.Identity{PhoneNumberID: channelID, TenantID: "t1"}
		},
		func(string) senderid.Resolver { return resolver },
		nil,
		call.Options{TmpDir: t.TempDir(), Now: time.Now},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
}

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

type hookResolver struct{ during func() }

func (h hookResolver) PNForLID(context.Context, types.JID) (types.JID, bool, error) {
	if h.during != nil {
		h.during()
	}
	return types.JID{}, false, nil
}

func TestCallEndingDuringTheIdentityLookupIsStillReported(t *testing.T) {
	pub := &memPublisher{}
	lc := &fakeCall{id: "C1", peer: "173907587899617:14@lid"}

	m := call.NewManager(pub, newMemStore(),
		func(channelID string) call.Identity {
			return call.Identity{PhoneNumberID: channelID, TenantID: "t1"}
		},
		func(string) senderid.Resolver {
			return hookResolver{during: func() { lc.fireEnd("cancelled") }}
		},
		nil,
		call.Options{TmpDir: t.TempDir(), Now: time.Now},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	caller := &fakeCaller{}
	m.Attach("chan-a", caller)
	caller.fireIncoming(lc)

	ended := pub.typed(call.EventEnded)
	if len(ended) != 1 {
		t.Fatalf("got %d ended events, want 1 for a call that ended while it was being tracked", len(ended))
	}
	if ended[0].Reason != "cancelled" {
		t.Errorf("reason = %q, want cancelled", ended[0].Reason)
	}

	if _, ok := m.Get("chan-a", "C1"); ok {
		t.Error("the call is still registered after it ended")
	}
}

func TestAnEarlyEndIsReportedAfterTheCallsArrival(t *testing.T) {
	pub := &memPublisher{}
	lc := &fakeCall{id: "C1", peer: "173907587899617:14@lid"}

	m := call.NewManager(pub, newMemStore(),
		func(channelID string) call.Identity {
			return call.Identity{PhoneNumberID: channelID, TenantID: "t1"}
		},
		func(string) senderid.Resolver {
			return hookResolver{during: func() { lc.fireEnd("cancelled") }}
		},
		nil,
		call.Options{TmpDir: t.TempDir(), Now: time.Now},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	caller := &fakeCaller{}
	m.Attach("chan-a", caller)
	caller.fireIncoming(lc)

	order := pub.sequence()
	if len(order) != 3 ||
		order[0] != "inbound" || order[1] != call.EventIncoming || order[2] != call.EventEnded {
		t.Errorf("publish order = %v, want [inbound incoming ended]", order)
	}
}
