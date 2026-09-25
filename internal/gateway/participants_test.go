package gateway_test

import (
	"context"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/w3nder/whatsmeow-gateway/internal/gateway"
)

type lidResolver map[string]string

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

	out := gateway.BuildGroupParticipants(context.Background(), lidResolver{"2002125877314": "5511999887766"}, "tenant-1", "channel-1", e)
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
	ia := gateway.BuildGroupParticipants(context.Background(), lidResolver{}, "t", "c", a)[0].EventID
	ib := gateway.BuildGroupParticipants(context.Background(), lidResolver{}, "t", "c", b)[0].EventID
	if ia != ib {
		t.Fatalf("same event in another order must dedupe: %s != %s", ia, ib)
	}
	c := groupEvent()
	c.Leave = a.Join
	if ic := gateway.BuildGroupParticipants(context.Background(), lidResolver{}, "t", "c", c)[0].EventID; ic == ia {
		t.Fatal("a leave must not collide with a join")
	}
}

func TestBuildGroupParticipantsLeaveVersusRemoved(t *testing.T) {
	left := groupEvent()
	self := types.NewJID("5511888887777", types.DefaultUserServer)
	left.Sender = &self
	left.Leave = []types.JID{self}
	if out := gateway.BuildGroupParticipants(context.Background(), lidResolver{}, "t", "c", left); out[0].Type != "leave" {
		t.Fatalf("self leave → %q", out[0].Type)
	}
	kicked := groupEvent()
	kicked.Leave = []types.JID{self}
	if out := gateway.BuildGroupParticipants(context.Background(), lidResolver{}, "t", "c", kicked); out[0].Type != "removed" {
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
	if out := gateway.BuildGroupParticipants(context.Background(), lidResolver{}, "t", "c", e); out[0].Type != "leave" {
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
	if out := gateway.BuildGroupParticipants(context.Background(), resolver, "t", "c", e); out[0].Type != "leave" {
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
	if out := gateway.BuildGroupParticipants(context.Background(), lidResolver{}, "t", "c", e); out[0].Type != "removed" {
		t.Fatalf("different phone leaving → %q", out[0].Type)
	}
}

func TestBuildGroupParticipantsSplitsJoinLeavePromoteDemote(t *testing.T) {
	e := groupEvent()
	e.Join = []types.JID{types.NewJID("1", types.DefaultUserServer)}
	e.Leave = []types.JID{types.NewJID("2", types.DefaultUserServer)}
	e.Promote = []types.JID{types.NewJID("3", types.DefaultUserServer)}
	e.Demote = []types.JID{types.NewJID("4", types.DefaultUserServer)}
	out := gateway.BuildGroupParticipants(context.Background(), lidResolver{}, "t", "c", e)
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
	if out := gateway.BuildGroupParticipants(context.Background(), lidResolver{}, "t", "c", e); len(out) != 0 {
		t.Fatalf("expected no events, got %+v", out)
	}
}
