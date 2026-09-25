package gateway_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/w3nder/whatsmeow-gateway/internal/gateway"
)

var discard = slog.New(slog.DiscardHandler)

type lidResolver map[string]string

type failingResolver struct{ err error }

func (r failingResolver) PNForLID(context.Context, types.JID) (types.JID, bool, error) {
	return types.JID{}, false, r.err
}

func (r lidResolver) PNForLID(_ context.Context, lid types.JID) (types.JID, bool, error) {
	if pn, ok := r[lid.User]; ok {
		return types.NewJID(pn, types.DefaultUserServer), true, nil
	}
	return types.JID{}, false, nil
}

func groupEvent() *events.GroupInfo {
	sender := types.NewJID("15550000000", types.DefaultUserServer)
	return &events.GroupInfo{
		JID:       types.NewJID("120363422547615282", types.GroupServer),
		Sender:    &sender,
		Timestamp: time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC),
	}
}

func TestBuildGroupParticipantsJoinResolvesPhoneAndLid(t *testing.T) {
	e := groupEvent()
	e.Join = []types.JID{types.NewJID("2002125877314", types.HiddenUserServer), types.NewJID("5511888887777", types.DefaultUserServer)}

	out := gateway.BuildGroupParticipants(context.Background(), lidResolver{"2002125877314": "5511999887766"}, discard, "tenant-1", "channel-1", e)
	if len(out) != 1 || out[0].Type != "join" || out[0].TenantID != "tenant-1" || out[0].GroupJID != "120363422547615282@g.us" {
		t.Fatalf("events %+v", out)
	}
	if out[0].OccurredAt != "2026-09-24T12:00:00Z" {
		t.Fatalf("occurredAt %q", out[0].OccurredAt)
	}
	p := out[0].Participants
	if p[0].JID != "2002125877314@lid" || p[0].LID != "2002125877314@lid" || p[0].Phone != "5511999887766" {
		t.Fatalf("lid participant %+v", p[0])
	}
	if p[1].JID != "5511888887777@s.whatsapp.net" || p[1].LID != "" || p[1].Phone != "5511888887777" {
		t.Fatalf("pn participant %+v", p[1])
	}
	if len(out[0].EventID) != 32 {
		t.Fatalf("eventId %q", out[0].EventID)
	}
}

func TestBuildGroupParticipantsEventIDIsDeterministicAndOrderInsensitive(t *testing.T) {
	a := groupEvent()
	a.Join = []types.JID{types.NewJID("1", types.DefaultUserServer), types.NewJID("2", types.DefaultUserServer)}
	b := groupEvent()
	b.Join = []types.JID{types.NewJID("2", types.DefaultUserServer), types.NewJID("1", types.DefaultUserServer)}
	ia := gateway.BuildGroupParticipants(context.Background(), lidResolver{}, discard, "t", "c", a)[0].EventID
	ib := gateway.BuildGroupParticipants(context.Background(), lidResolver{}, discard, "t", "c", b)[0].EventID
	if ia != ib {
		t.Fatalf("same event in another order must dedupe: %s != %s", ia, ib)
	}
	c := groupEvent()
	c.Leave = a.Join
	if ic := gateway.BuildGroupParticipants(context.Background(), lidResolver{}, discard, "t", "c", c)[0].EventID; ic == ia {
		t.Fatal("a leave must not collide with a join")
	}
}

func TestBuildGroupParticipantsLeaveVersusRemoved(t *testing.T) {
	left := groupEvent()
	self := types.NewJID("5511888887777", types.DefaultUserServer)
	left.Sender = &self
	left.Leave = []types.JID{self}
	if out := gateway.BuildGroupParticipants(context.Background(), lidResolver{}, discard, "t", "c", left); out[0].Type != "leave" {
		t.Fatalf("self leave → %q", out[0].Type)
	}
	kicked := groupEvent()
	kicked.Leave = []types.JID{self}
	if out := gateway.BuildGroupParticipants(context.Background(), lidResolver{}, discard, "t", "c", kicked); out[0].Type != "removed" {
		t.Fatalf("admin removal → %q", out[0].Type)
	}
}

func TestBuildGroupParticipantsLeaveMatchesSenderPNWhenSenderIsLID(t *testing.T) {
	e := groupEvent()
	senderLID := types.NewJID("2002125877314", types.HiddenUserServer)
	senderPN := types.NewJID("5511888887777", types.DefaultUserServer)
	e.Sender = &senderLID
	e.SenderPN = &senderPN
	e.Leave = []types.JID{senderPN}
	if out := gateway.BuildGroupParticipants(context.Background(), lidResolver{}, discard, "t", "c", e); out[0].Type != "leave" {
		t.Fatalf("sender LID with SenderPN leaving as phone → %q", out[0].Type)
	}
}

func TestBuildGroupParticipantsLeaveResolvesLIDAgainstPhoneSender(t *testing.T) {
	e := groupEvent()
	senderPN := types.NewJID("5511888887777", types.DefaultUserServer)
	leavingLID := types.NewJID("2002125877314", types.HiddenUserServer)
	e.Sender = &senderPN
	e.Leave = []types.JID{leavingLID}
	resolver := lidResolver{"2002125877314": "5511888887777"}
	if out := gateway.BuildGroupParticipants(context.Background(), resolver, discard, "t", "c", e); out[0].Type != "leave" {
		t.Fatalf("sender phone leaving as resolved LID → %q", out[0].Type)
	}
}

func TestBuildGroupParticipantsLeaveDifferentPhoneIsRemoved(t *testing.T) {
	e := groupEvent()
	senderLID := types.NewJID("2002125877314", types.HiddenUserServer)
	senderPN := types.NewJID("5511888887777", types.DefaultUserServer)
	e.Sender = &senderLID
	e.SenderPN = &senderPN
	e.Leave = []types.JID{types.NewJID("5511777776666", types.DefaultUserServer)}
	if out := gateway.BuildGroupParticipants(context.Background(), lidResolver{}, discard, "t", "c", e); out[0].Type != "removed" {
		t.Fatalf("different phone leaving → %q", out[0].Type)
	}
}

func TestBuildGroupParticipantsSplitsJoinLeavePromoteDemote(t *testing.T) {
	e := groupEvent()
	e.Join = []types.JID{types.NewJID("1", types.DefaultUserServer)}
	e.Leave = []types.JID{types.NewJID("2", types.DefaultUserServer)}
	e.Promote = []types.JID{types.NewJID("3", types.DefaultUserServer)}
	e.Demote = []types.JID{types.NewJID("4", types.DefaultUserServer)}
	out := gateway.BuildGroupParticipants(context.Background(), lidResolver{}, discard, "t", "c", e)
	kinds := []string{}
	for _, evt := range out {
		kinds = append(kinds, evt.Type)
	}
	if len(out) != 4 || kinds[0] != "join" || kinds[1] != "removed" || kinds[2] != "promoted" || kinds[3] != "demoted" {
		t.Fatalf("kinds %v", kinds)
	}
}

func TestBuildGroupParticipantsIgnoresNameOnlyChanges(t *testing.T) {
	e := groupEvent()
	e.Name = &types.GroupName{Name: "x"}
	if out := gateway.BuildGroupParticipants(context.Background(), lidResolver{}, discard, "t", "c", e); len(out) != 0 {
		t.Fatalf("expected no events, got %+v", out)
	}
}

func TestBuildGroupParticipantsTypesEachLeaverOnItsOwn(t *testing.T) {
	e := groupEvent()
	self := types.NewJID("5511888887777", types.DefaultUserServer)
	kicked := types.NewJID("5511777776666", types.DefaultUserServer)
	e.Sender = &self
	e.Leave = []types.JID{kicked, self}
	out := gateway.BuildGroupParticipants(context.Background(), lidResolver{}, discard, "t", "c", e)
	if len(out) != 2 {
		t.Fatalf("one leave with a self-leaver and a removed member must split in two events, got %+v", out)
	}
	if out[0].Type != "leave" || len(out[0].Participants) != 1 || out[0].Participants[0].Phone != self.User {
		t.Fatalf("leave %+v", out[0])
	}
	if out[1].Type != "removed" || len(out[1].Participants) != 1 || out[1].Participants[0].Phone != kicked.User {
		t.Fatalf("removed %+v", out[1])
	}
	if out[0].EventID == out[1].EventID {
		t.Fatal("the two halves of one leave must not share an eventId")
	}
}

func TestBuildGroupParticipantsLogsAFailedLIDResolutionWithContext(t *testing.T) {
	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, nil))
	e := groupEvent()
	lid := types.NewJID("2002125877314", types.HiddenUserServer)
	e.Join = []types.JID{lid}
	e.Leave = []types.JID{lid}

	out := gateway.BuildGroupParticipants(context.Background(), failingResolver{errors.New("lid store down")}, log, "t", "channel-1", e)
	if len(out) != 2 || out[0].Participants[0].Phone != "" || out[1].Type != "removed" {
		t.Fatalf("an unresolved lid keeps only its jid and never counts as a self leave, got %+v", out)
	}
	line := logs.String()
	for _, want := range []string{"lid store down", "channel-1", "120363422547615282@g.us", "2002125877314@lid"} {
		if !strings.Contains(line, want) {
			t.Fatalf("the failed resolution must be logged with %q, logs: %s", want, line)
		}
	}
	if n := strings.Count(line, "lid store down"); n != 1 {
		t.Fatalf("the same lid must be resolved once per event, logged %d times", n)
	}
}
