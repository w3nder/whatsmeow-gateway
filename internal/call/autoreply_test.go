package call_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/w3nder/whatsmeow-gateway/internal/call"
	"github.com/w3nder/whatsmeow-gateway/internal/channelsettings"
)

type memReplier struct {
	mu      sync.Mutex
	replies []call.AutoReply
	err     error
}

func (r *memReplier) Reply(_ context.Context, reply call.AutoReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.replies = append(r.replies, reply)
	return r.err
}

func (r *memReplier) sent() []call.AutoReply {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]call.AutoReply(nil), r.replies...)
}

func newAutoReplyManager(t *testing.T, pub call.Publisher, settings call.SettingsSource, replier call.AutoReplier, now func() time.Time) *call.Manager {
	t.Helper()
	return call.NewManager(pub, newMemStore(),
		func(channelID string) call.Identity {
			return call.Identity{PhoneNumberID: channelID, TenantID: "t1"}
		},
		nil,
		nil,
		call.Options{TmpDir: t.TempDir(), Now: now, Settings: settings, Replier: replier},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
}

func messageSettings(channelID, message string) *channelsettings.Map {
	settings := channelsettings.NewMap()
	settings.Set(channelID, channelsettings.Settings{ListenGroups: true, ReceiveCalls: false, CallRejectMessage: message})
	return settings
}

const rejectText = "Não atendemos ligações, escreva aqui."

func TestCooldownAllowsTheFirstAndBlocksTheRepeatUntilTheWindowPasses(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1754300000, 0)}
	c := call.NewCooldown(call.CallRejectMessageCooldown, clock.Now)

	if !c.Allow("chan-a", "5511888887777") {
		t.Fatal("the first call of a caller must be allowed")
	}
	clock.advance(call.CallRejectMessageCooldown - time.Second)
	if c.Allow("chan-a", "5511888887777") {
		t.Fatal("a repeat inside the window must be blocked")
	}
	clock.advance(time.Second)
	if !c.Allow("chan-a", "5511888887777") {
		t.Fatal("at exactly the window the caller may be answered again")
	}
}

func TestCooldownIsPerChannelAndPerCaller(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1754300000, 0)}
	c := call.NewCooldown(call.CallRejectMessageCooldown, clock.Now)

	c.Allow("chan-a", "5511888887777")

	if !c.Allow("chan-a", "5511999996666") {
		t.Error("another caller on the same channel must be allowed")
	}
	if !c.Allow("chan-b", "5511888887777") {
		t.Error("the same caller on another channel must be allowed")
	}
}

func TestCooldownForgetsExpiredEntries(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1754300000, 0)}
	c := call.NewCooldown(time.Minute, clock.Now)

	c.Allow("chan-a", "old")
	clock.advance(2 * time.Minute)
	if !c.Allow("chan-a", "new") {
		t.Fatal("a new caller must be allowed")
	}
	if !c.Allow("chan-a", "old") {
		t.Fatal("an expired caller must be allowed again")
	}
}

func TestAutoRejectSendsTheConfiguredMessageToTheOneToOneCaller(t *testing.T) {
	pub := &memPublisher{}
	replier := &memReplier{}
	m := newAutoReplyManager(t, pub, messageSettings("chan-a", rejectText), replier, time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	caller.fireIncoming(&fakeCall{id: "C1", peer: "5511888887777@s.whatsapp.net"})
	m.WaitForReplies(5 * time.Second)

	want := []call.AutoReply{{ChannelID: "chan-a", CallID: "C1", SenderPn: "5511888887777", Text: rejectText}}
	if got := replier.sent(); !reflect.DeepEqual(got, want) {
		t.Fatalf("replies = %+v, want %+v", got, want)
	}
	if inbound := pub.inboundEvents(); len(inbound) != 1 || inbound[0].RichContent.State != call.InboundStateAutoRejected {
		t.Fatalf("the call must still be published as auto_rejected, got %+v", inbound)
	}
}

func TestAutoRejectWithoutAMessageSendsNothing(t *testing.T) {
	replier := &memReplier{}
	m := newAutoReplyManager(t, &memPublisher{}, messageSettings("chan-a", ""), replier, time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	caller.fireIncoming(&fakeCall{id: "C1", peer: "5511888887777@s.whatsapp.net"})
	m.WaitForReplies(5 * time.Second)

	if got := replier.sent(); len(got) != 0 {
		t.Fatalf("no message configured, replies = %+v", got)
	}
}

func TestAutoRejectOfAGroupCallSendsNoMessageAndKeepsTheCooldownFree(t *testing.T) {
	pub := &memPublisher{}
	replier := &memReplier{}
	m := newAutoReplyManager(t, pub, messageSettings("chan-a", rejectText), replier, time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)
	group := &fakeCall{id: "G1", peer: "5511888887777@s.whatsapp.net", group: true}

	caller.fireIncoming(group)
	caller.fireIncoming(&fakeCall{id: "C2", peer: "5511888887777@s.whatsapp.net"})
	m.WaitForReplies(5 * time.Second)

	if got := group.recordedActions(); !reflect.DeepEqual(got, []string{"reject"}) {
		t.Fatalf("the group call must be rejected, actions = %v", got)
	}
	got := replier.sent()
	if len(got) != 1 || got[0].CallID != "C2" {
		t.Fatalf("only the one-to-one call is answered, replies = %+v", got)
	}
	if inbound := pub.inboundEvents(); len(inbound) != 2 {
		t.Fatalf("both calls are published as auto_rejected, got %d events", len(inbound))
	}
}

func TestAutoRejectAnswersACallerOnlyOncePerCooldownWindow(t *testing.T) {
	pub := &memPublisher{}
	replier := &memReplier{}
	clock := &fakeClock{now: time.Unix(1754300000, 0)}
	m := newAutoReplyManager(t, pub, messageSettings("chan-a", rejectText), replier, clock.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)
	peer := "5511888887777@s.whatsapp.net"

	caller.fireIncoming(&fakeCall{id: "C1", peer: peer})
	clock.advance(5 * time.Minute)
	caller.fireIncoming(&fakeCall{id: "C2", peer: peer})
	m.WaitForReplies(5 * time.Second)

	if got := replier.sent(); len(got) != 1 || got[0].CallID != "C1" {
		t.Fatalf("inside the window only the first call is answered, replies = %+v", got)
	}
	if inbound := pub.inboundEvents(); len(inbound) != 2 {
		t.Fatalf("both calls are still published as auto_rejected, got %d events", len(inbound))
	}

	clock.advance(5 * time.Minute)
	caller.fireIncoming(&fakeCall{id: "C3", peer: peer})
	m.WaitForReplies(5 * time.Second)

	if got := replier.sent(); len(got) != 2 || got[1].CallID != "C3" {
		t.Fatalf("after the window the caller is answered again, replies = %+v", got)
	}
}

func TestAutoRejectFailureSendsNoMessageAndConsumesNoCooldown(t *testing.T) {
	replier := &memReplier{}
	m := newAutoReplyManager(t, &memPublisher{}, messageSettings("chan-a", rejectText), replier, time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)
	peer := "5511888887777@s.whatsapp.net"

	caller.fireIncoming(&fakeCall{id: "C1", peer: peer, rejectErr: errors.New("socket closed")})
	m.WaitForReplies(5 * time.Second)
	if got := replier.sent(); len(got) != 0 {
		t.Fatalf("a failed reject must send nothing, replies = %+v", got)
	}

	caller.fireIncoming(&fakeCall{id: "C2", peer: peer})
	m.WaitForReplies(5 * time.Second)
	if got := replier.sent(); len(got) != 1 || got[0].CallID != "C2" {
		t.Fatalf("the failed attempt must not have used the cooldown, replies = %+v", got)
	}
}

func TestAutoRejectKeepsWorkingWhenTheReplierFails(t *testing.T) {
	pub := &memPublisher{}
	replier := &memReplier{err: errors.New("whatsapp unreachable")}
	m := newAutoReplyManager(t, pub, messageSettings("chan-a", rejectText), replier, time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	caller.fireIncoming(&fakeCall{id: "C1", peer: "5511888887777@s.whatsapp.net"})
	m.WaitForReplies(5 * time.Second)

	if inbound := pub.inboundEvents(); len(inbound) != 1 {
		t.Fatalf("the rejected call is published even if the message fails, got %d events", len(inbound))
	}
}

func TestAutoRejectUsesTheLidAsTheCooldownKeyWhenThePhoneIsUnknown(t *testing.T) {
	replier := &memReplier{}
	m := newAutoReplyManager(t, &memPublisher{}, messageSettings("chan-a", rejectText), replier, time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	caller.fireIncoming(&fakeCall{id: "C1", peer: "173907587899617:14@lid"})
	caller.fireIncoming(&fakeCall{id: "C2", peer: "173907587899617:9@lid"})
	m.WaitForReplies(5 * time.Second)

	got := replier.sent()
	if len(got) != 1 || got[0].SenderLid != "173907587899617" || got[0].SenderPn != "" {
		t.Fatalf("two calls from the same lid must produce one reply carrying the lid, got %+v", got)
	}
}
