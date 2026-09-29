package call_test

import (
	"errors"
	"io"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/w3nder/whatsmeow-gateway/internal/call"
	"github.com/w3nder/whatsmeow-gateway/internal/channelsettings"
)

func newAutoRejectManager(t *testing.T, pub call.Publisher, settings call.SettingsSource, now func() time.Time) *call.Manager {
	t.Helper()
	return call.NewManager(pub, newMemStore(),
		func(channelID string) call.Identity {
			return call.Identity{PhoneNumberID: channelID, TenantID: "t1"}
		},
		nil,
		nil,
		call.Options{TmpDir: t.TempDir(), Now: now, Settings: settings},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
}

func rejectingSettings(channelID string) *channelsettings.Map {
	settings := channelsettings.NewMap()
	settings.Set(channelID, channelsettings.Settings{ListenGroups: true, ReceiveCalls: false})
	return settings
}

func TestAutoRejectRejectsTheCallAndPublishesTheAutoRejectedState(t *testing.T) {
	pub := &memPublisher{}
	m := newAutoRejectManager(t, pub, rejectingSettings("chan-a"), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)
	lc := &fakeCall{id: "C1", peer: "5511888887777@s.whatsapp.net", video: true}

	caller.fireIncoming(lc)

	if got := lc.recordedActions(); !reflect.DeepEqual(got, []string{"reject"}) {
		t.Fatalf("actions = %v, want only reject", got)
	}
	inbound := pub.inboundEvents()
	if len(inbound) != 1 {
		t.Fatalf("got %d inbound events, want 1", len(inbound))
	}
	evt := inbound[0]
	if evt.RichContent == nil || evt.RichContent.State != call.InboundStateAutoRejected {
		t.Fatalf("richContent = %+v, want state auto_rejected", evt.RichContent)
	}
	if evt.RichContent.Direction != call.DirectionInbound || !evt.RichContent.IsVideo {
		t.Errorf("richContent = %+v, want an inbound video call", evt.RichContent)
	}
	if evt.ProviderMessageID != "C1" || evt.From != "5511888887777" || evt.FromMe {
		t.Errorf("event = %+v, want the caller as sender, call id as provider id, not fromMe", evt)
	}
	if evt.TenantID != "t1" || evt.PhoneNumberID != "chan-a" {
		t.Errorf("identity = %q/%q, want t1/chan-a", evt.TenantID, evt.PhoneNumberID)
	}
}

func TestAutoRejectNeverTracksNorOffersTheCallToTheOperator(t *testing.T) {
	pub := &memPublisher{}
	m := newAutoRejectManager(t, pub, rejectingSettings("chan-a"), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	caller.fireIncoming(&fakeCall{id: "C1", peer: "5511888887777@s.whatsapp.net"})

	if incoming := pub.typed(call.EventIncoming); len(incoming) != 0 {
		t.Fatalf("an auto rejected call must not be offered, got %d incoming events", len(incoming))
	}
	if _, ok := m.Get("chan-a", "C1"); ok {
		t.Fatal("an auto rejected call must not be tracked")
	}
}

func TestAutoRejectOffKeepsTheNormalFlow(t *testing.T) {
	pub := &memPublisher{}
	m := newAutoRejectManager(t, pub, channelsettings.NewMap(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)
	lc := &fakeCall{id: "C1", peer: "5511888887777@s.whatsapp.net"}

	caller.fireIncoming(lc)

	if got := lc.recordedActions(); len(got) != 0 {
		t.Fatalf("a channel that receives calls must not reject, actions = %v", got)
	}
	if _, ok := m.Get("chan-a", "C1"); !ok {
		t.Error("the call must be tracked as before")
	}
	inbound := pub.inboundEvents()
	if len(inbound) != 1 || inbound[0].RichContent.State != "ringing" {
		t.Fatalf("inbound = %+v, want one ringing event", inbound)
	}
}

func TestAutoRejectOnlyAppliesToTheChannelThatTurnedCallsOff(t *testing.T) {
	pub := &memPublisher{}
	m := newAutoRejectManager(t, pub, rejectingSettings("chan-a"), time.Now)
	callerA, callerB := &fakeCaller{}, &fakeCaller{}
	m.Attach("chan-a", callerA)
	m.Attach("chan-b", callerB)
	lcB := &fakeCall{id: "CB", peer: "5511888887777@s.whatsapp.net"}

	callerB.fireIncoming(lcB)

	if got := lcB.recordedActions(); len(got) != 0 {
		t.Fatalf("chan-b receives calls, actions = %v", got)
	}
	if _, ok := m.Get("chan-b", "CB"); !ok {
		t.Error("chan-b call must be tracked")
	}
}

func TestAutoRejectFailureLeavesTheCallToTheOperatorWithoutARejectedState(t *testing.T) {
	pub := &memPublisher{}
	m := newAutoRejectManager(t, pub, rejectingSettings("chan-a"), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)
	lc := &fakeCall{id: "C1", peer: "5511888887777@s.whatsapp.net", rejectErr: errors.New("send reject: socket closed")}

	caller.fireIncoming(lc)

	inbound := pub.inboundEvents()
	if len(inbound) != 1 || inbound[0].RichContent.State != "ringing" {
		t.Fatalf("inbound = %+v, want a single ringing event and no auto_rejected", inbound)
	}
	if _, ok := m.Get("chan-a", "C1"); !ok {
		t.Error("a call whose reject failed must follow the normal flow")
	}
}
