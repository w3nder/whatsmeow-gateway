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

func allowed(c *call.Cooldown, channelID string, callers ...string) bool {
	_, ok := c.Allow(channelID, callers...)
	return ok
}

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

	if !allowed(c, "chan-a", "5511888887777") {
		t.Fatal("the first call of a caller must be allowed")
	}
	clock.advance(call.CallRejectMessageCooldown - time.Second)
	if allowed(c, "chan-a", "5511888887777") {
		t.Fatal("a repeat inside the window must be blocked")
	}
	clock.advance(time.Second)
	if !allowed(c, "chan-a", "5511888887777") {
		t.Fatal("at exactly the window the caller may be answered again")
	}
}

func TestCooldownIsPerChannelAndPerCaller(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1754300000, 0)}
	c := call.NewCooldown(call.CallRejectMessageCooldown, clock.Now)

	allowed(c, "chan-a", "5511888887777")

	if !allowed(c, "chan-a", "5511999996666") {
		t.Error("another caller on the same channel must be allowed")
	}
	if !allowed(c, "chan-b", "5511888887777") {
		t.Error("the same caller on another channel must be allowed")
	}
}

func TestCooldownForgetsExpiredEntries(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1754300000, 0)}
	c := call.NewCooldown(time.Minute, clock.Now)

	allowed(c, "chan-a", "old")
	clock.advance(2 * time.Minute)
	if !allowed(c, "chan-a", "new") {
		t.Fatal("a new caller must be allowed")
	}
	if !allowed(c, "chan-a", "old") {
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

func TestCooldownTreatsEveryKnownIdentityOfTheCallerAsTheSameCaller(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1754300000, 0)}
	c := call.NewCooldown(call.CallRejectMessageCooldown, clock.Now)

	if !allowed(c, "chan-a", "173907587899617", "5511888887777") {
		t.Fatal("the first call must be allowed")
	}
	if allowed(c, "chan-a", "", "5511888887777") {
		t.Error("the same caller seen only by phone must be blocked")
	}
	if allowed(c, "chan-a", "173907587899617", "") {
		t.Error("the same caller seen only by lid must be blocked")
	}
	if !allowed(c, "chan-a", "999", "5511000000000") {
		t.Error("an unrelated caller must be allowed")
	}
}

func TestCooldownReservationBlocksAnImmediateSecondAllow(t *testing.T) {
	c := call.NewCooldown(call.CallRejectMessageCooldown, time.Now)

	first, second := allowed(c, "chan-a", "5511888887777"), allowed(c, "chan-a", "5511888887777")
	if !first || second {
		t.Fatalf("allow results = %v, %v, want true then false", first, second)
	}
}

func TestCooldownReleaseFreesEveryKeyOfTheReservation(t *testing.T) {
	c := call.NewCooldown(call.CallRejectMessageCooldown, time.Now)

	held, _ := c.Allow("chan-a", "173907587899617", "5511888887777")
	c.Release(held)

	if !allowed(c, "chan-a", "", "5511888887777") || !allowed(c, "chan-a", "173907587899617", "") {
		t.Fatal("a released reservation must free both identities")
	}
}

func TestAutoRejectReleasesTheCooldownWhenTheMessageFailsToSend(t *testing.T) {
	replier := &memReplier{err: errors.New("whatsapp unreachable")}
	m := newAutoReplyManager(t, &memPublisher{}, messageSettings("chan-a", rejectText), replier, time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)
	peer := "5511888887777@s.whatsapp.net"

	caller.fireIncoming(&fakeCall{id: "C1", peer: peer})
	m.WaitForReplies(5 * time.Second)
	caller.fireIncoming(&fakeCall{id: "C2", peer: peer})
	m.WaitForReplies(5 * time.Second)

	if got := len(replier.sent()); got != 2 {
		t.Fatalf("a failed send must not suppress the next call, attempts = %d", got)
	}
}

func TestCooldownStaleReleaseKeepsTheNewerReservationOfTheSameKeys(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1754300000, 0)}
	c := call.NewCooldown(call.CallRejectMessageCooldown, clock.Now)

	stale, _ := c.Allow("chan-a", "173907587899617", "5511888887777")
	clock.advance(call.CallRejectMessageCooldown)
	fresh, ok := c.Allow("chan-a", "173907587899617", "5511888887777")
	if !ok {
		t.Fatal("the window passed, the caller must be reservable again")
	}

	c.Release(stale)

	if allowed(c, "chan-a", "5511888887777") || allowed(c, "chan-a", "173907587899617") {
		t.Fatal("a stale release must not free the newer reservation")
	}
	c.Release(fresh)
	if !allowed(c, "chan-a", "5511888887777") {
		t.Fatal("the owner of the reservation must still release it")
	}
}

type hangingReplier struct {
	mu       sync.Mutex
	attempts int
}

func (r *hangingReplier) Reply(ctx context.Context, _ call.AutoReply) error {
	r.mu.Lock()
	r.attempts++
	first := r.attempts == 1
	r.mu.Unlock()
	if first {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func (r *hangingReplier) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.attempts
}

func TestAutoRejectSendThatExceedsTheTimeoutReleasesTheCooldown(t *testing.T) {
	replier := &hangingReplier{}
	m := call.NewManager(&memPublisher{}, newMemStore(),
		func(channelID string) call.Identity {
			return call.Identity{PhoneNumberID: channelID, TenantID: "t1"}
		},
		nil,
		nil,
		call.Options{
			TmpDir:       t.TempDir(),
			Now:          time.Now,
			Settings:     messageSettings("chan-a", rejectText),
			Replier:      replier,
			ReplyTimeout: 50 * time.Millisecond,
		},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)
	peer := "5511888887777@s.whatsapp.net"

	caller.fireIncoming(&fakeCall{id: "C1", peer: peer})
	m.WaitForReplies(5 * time.Second)
	caller.fireIncoming(&fakeCall{id: "C2", peer: peer})
	m.WaitForReplies(5 * time.Second)

	if got := replier.count(); got != 2 {
		t.Fatalf("a hung send must time out and free the next call, attempts = %d", got)
	}
}
