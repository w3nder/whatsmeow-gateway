package session

import (
	"testing"

	"github.com/purpshell/meowcaller"
	"go.mau.fi/whatsmeow/types"

	"github.com/w3nder/whatsmeow-gateway/internal/call"
)

// The library's Call and Client are driven by a live VoIP stack and cannot be
// built in a test, so what is covered here is what is testable without one: the
// conversion of every value the adapter passes across the boundary.

func TestPhaseFromLibrary(t *testing.T) {
	cases := map[meowcaller.CallPhase]call.Phase{
		meowcaller.CallPhaseIdle:        call.PhaseIdle,
		meowcaller.CallPhaseCalling:     call.PhaseCalling,
		meowcaller.CallPhaseRinging:     call.PhaseRinging,
		meowcaller.CallPhaseConnecting:  call.PhaseConnecting,
		meowcaller.CallPhaseActive:      call.PhaseActive,
		meowcaller.CallPhaseEnded:       call.PhaseEnded,
		meowcaller.CallPhaseWaitingRoom: call.PhaseWaitingRoom,
	}
	for lib, want := range cases {
		if got := phaseFrom(lib); got != want {
			t.Errorf("phaseFrom(%v) = %v, want %v", lib, got, want)
		}
	}
}

func TestVideoStateFromLibrary(t *testing.T) {
	got := videoStateFrom(meowcaller.VideoState{Active: true, Upgrade: true, Orientation: 2, Raw: 11})
	want := call.VideoState{Active: true, Upgrade: true, Orientation: 2, Raw: 11}
	if got != want {
		t.Errorf("videoStateFrom = %+v, want %+v", got, want)
	}
}

func TestReactionFromLibraryFlattensJIDs(t *testing.T) {
	sender := types.NewJID("5511888888888", types.DefaultUserServer)
	got := reactionFrom(meowcaller.CallReaction{Emoji: "👍", Sender: sender, Removed: true})
	if got.Emoji != "👍" || got.Sender != sender.String() || !got.Removed {
		t.Errorf("reactionFrom = %+v, want the flattened reaction", got)
	}
}

func TestGroupStateFromLibraryFlattensParticipants(t *testing.T) {
	jid := types.NewJID("5511888888888", types.DefaultUserServer)
	pn := types.NewJID("5511777777777", types.DefaultUserServer)
	got := groupStateFrom(meowcaller.GroupCallState{
		TransactionID: 7,
		Participants: []meowcaller.GroupCallParticipant{
			{JID: jid, PN: pn, State: "connected", HandRaised: true},
		},
	})
	if got.TransactionID != 7 || len(got.Participants) != 1 {
		t.Fatalf("groupStateFrom = %+v", got)
	}
	p := got.Participants[0]
	if p.JID != jid.String() || p.PN != pn.String() || p.State != "connected" || !p.HandRaised {
		t.Errorf("participant = %+v, want the flattened participant", p)
	}
}

func TestWaitingRoomFromLibrary(t *testing.T) {
	jid := types.NewJID("5511888888888", types.DefaultUserServer)
	got := waitingRoomFrom(meowcaller.WaitingRoomState{
		Enabled:       true,
		IsAdmin:       true,
		TransactionID: 3,
		Users:         []meowcaller.WaitingRoomUser{{JID: jid, State: "pending"}},
	})
	if !got.Enabled || !got.IsAdmin || got.TransactionID != 3 {
		t.Errorf("waitingRoomFrom = %+v, want the admin-enabled room", got)
	}
	if len(got.Users) != 1 || got.Users[0].JID != jid.String() {
		t.Errorf("users = %+v, want the pending user", got.Users)
	}
}

func TestHandAndScreenShareFromLibrary(t *testing.T) {
	jid := types.NewJID("5511888888888", types.DefaultUserServer)

	hand := handStateFrom(meowcaller.HandRaiseState{Participant: jid, Raised: true})
	if hand.Participant != jid.String() || !hand.Raised {
		t.Errorf("handStateFrom = %+v, want the raised hand", hand)
	}

	share := screenShareFrom(meowcaller.ScreenShareState{Participant: jid, Active: true, Version: 4})
	if share.Participant != jid.String() || !share.Active || share.Version != 4 {
		t.Errorf("screenShareFrom = %+v, want the active share", share)
	}
}

// An absent JID must render as empty rather than as a bare "@server".
func TestJIDStringKeepsEmptyEmpty(t *testing.T) {
	if got := jidString(types.EmptyJID); got != "" {
		t.Errorf("jidString(empty) = %q, want an empty string", got)
	}
	jid := types.NewJID("5511888888888", types.DefaultUserServer)
	if got := jidString(jid); got != jid.String() {
		t.Errorf("jidString = %q, want %q", got, jid.String())
	}
}
