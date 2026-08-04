package call_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/call"
)

func noFetch(context.Context, string) ([]byte, error) { return nil, nil }

// dispatchOnLiveCall sets up a manager with one live inbound call and runs cmd
// against it.
func dispatchOnLiveCall(t *testing.T, cmd amqp.GatewayCallCommand) (*memPublisher, *fakeCall) {
	t.Helper()
	pub := &memPublisher{}
	m := newTestManager(t, pub, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)
	lc := &fakeCall{id: "C1", peer: "5511888888888@s.whatsapp.net"}
	caller.fireIncoming(lc)

	if err := m.Dispatch(context.Background(), caller, cmd, noFetch); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	return pub, lc
}

func TestDispatchRoutesActionsToTheCall(t *testing.T) {
	cases := []string{
		"answer",
		"reject",
		"hangup",
		"video.start",
		"video.accept",
		"video.stop",
		"video.enable",
		"video.orientation",
		"reaction",
		"hand.raise",
		"screenshare.start",
		"screenshare.stop",
		"participant.add",
		"participant.ring",
		"approval.set",
		"participant.admit",
		"participant.deny",
	}
	for _, action := range cases {
		t.Run(action, func(t *testing.T) {
			pub, lc := dispatchOnLiveCall(t, amqp.GatewayCallCommand{
				ChannelID: "chan-a", CallID: "C1", CommandID: "cmd-1", Action: action,
				Emoji: "👍", Participant: "p@s.whatsapp.net",
			})
			actions := lc.recordedActions()
			if len(actions) != 1 || actions[0] != action {
				t.Errorf("actions = %v, want [%s]", actions, action)
			}
			acks := pub.typed(call.EventCommandAck)
			if len(acks) != 1 || acks[0].CommandID != "cmd-1" {
				t.Errorf("acks = %+v, want one carrying cmd-1", acks)
			}
			if failed := pub.typed(call.EventCommandFailed); len(failed) != 0 {
				t.Errorf("failures = %+v, want none", failed)
			}
		})
	}
}

func TestDispatchPassesArgumentsThrough(t *testing.T) {
	_, lc := dispatchOnLiveCall(t, amqp.GatewayCallCommand{
		ChannelID: "chan-a", CallID: "C1", CommandID: "cmd-1",
		Action: "reaction", Emoji: "🎉",
	})
	if got := lc.recordedArg(); got != "🎉" {
		t.Errorf("reaction emoji = %q, want 🎉", got)
	}

	_, lc = dispatchOnLiveCall(t, amqp.GatewayCallCommand{
		ChannelID: "chan-a", CallID: "C1", CommandID: "cmd-2",
		Action: "participant.add", Participant: "new@s.whatsapp.net",
	})
	if got := lc.recordedArg(); got != "new@s.whatsapp.net" {
		t.Errorf("participant = %q, want new@s.whatsapp.net", got)
	}
}

// An unknown call-id must fail loudly and must not be retried: requeueing it
// would loop until the DLQ.
func TestDispatchUnknownCallFails(t *testing.T) {
	pub := &memPublisher{}
	m := newTestManager(t, pub, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	err := m.Dispatch(context.Background(), caller, amqp.GatewayCallCommand{
		ChannelID: "chan-a", CallID: "NOPE", CommandID: "cmd-1", Action: "hangup",
	}, noFetch)
	if err != nil {
		t.Fatalf("Dispatch returned %v, want nil (a bad command is reported, not retried)", err)
	}
	failed := pub.typed(call.EventCommandFailed)
	if len(failed) != 1 || failed[0].Error == nil || failed[0].Error.Code != call.CodeCallNotFound {
		t.Errorf("failures = %+v, want one call_not_found", failed)
	}
}

func TestDispatchUnknownActionFails(t *testing.T) {
	pub, _ := dispatchOnLiveCall(t, amqp.GatewayCallCommand{
		ChannelID: "chan-a", CallID: "C1", CommandID: "cmd-1", Action: "teleport",
	})
	failed := pub.typed(call.EventCommandFailed)
	if len(failed) != 1 || failed[0].Error.Code != call.CodeUnknownAction {
		t.Errorf("failures = %+v, want one unknown_action", failed)
	}
}

// A failing action is reported, not retried.
func TestDispatchActionErrorIsReported(t *testing.T) {
	pub := &memPublisher{}
	m := newTestManager(t, pub, newMemStore(), time.Now)
	caller := &fakeCaller{callErr: errors.New("no route to peer")}

	if err := m.Dispatch(context.Background(), caller, amqp.GatewayCallCommand{
		ChannelID: "chan-a", CommandID: "cmd-1", Action: "dial", To: "+5511888888888",
	}, noFetch); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	failed := pub.typed(call.EventCommandFailed)
	if len(failed) != 1 || failed[0].Error.Code != call.CodeActionFailed {
		t.Fatalf("failures = %+v, want one action_failed", failed)
	}
	if failed[0].Error.Reason == "" {
		t.Error("action_failed carries no reason")
	}
}

func TestDispatchDialTracksTheOutboundCall(t *testing.T) {
	pub := &memPublisher{}
	m := newTestManager(t, pub, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	if err := m.Dispatch(context.Background(), caller, amqp.GatewayCallCommand{
		ChannelID: "chan-a", CommandID: "cmd-1", Action: "dial",
		To: "+5511888888888", Video: true,
	}, noFetch); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	tracked, ok := m.Get("chan-a", "OUT1")
	if !ok {
		t.Fatal("outbound call was not registered")
	}
	if tracked.Direction != call.DirectionOutbound {
		t.Errorf("Direction = %q, want outbound", tracked.Direction)
	}
	ringing := pub.typed(call.EventRinging)
	if len(ringing) != 1 || !ringing[0].IsVideo {
		t.Errorf("ringing = %+v, want one video ringing event", ringing)
	}
}

func TestDispatchDialWithoutTargetFails(t *testing.T) {
	pub := &memPublisher{}
	m := newTestManager(t, pub, newMemStore(), time.Now)
	caller := &fakeCaller{}

	if err := m.Dispatch(context.Background(), caller, amqp.GatewayCallCommand{
		ChannelID: "chan-a", CommandID: "cmd-1", Action: "dial",
	}, noFetch); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	failed := pub.typed(call.EventCommandFailed)
	if len(failed) != 1 || failed[0].Error.Code != call.CodeInvalidTarget {
		t.Errorf("failures = %+v, want one invalid_target", failed)
	}
}

func TestDispatchGroupDialNeedsTwoTargets(t *testing.T) {
	pub := &memPublisher{}
	m := newTestManager(t, pub, newMemStore(), time.Now)
	caller := &fakeCaller{}

	if err := m.Dispatch(context.Background(), caller, amqp.GatewayCallCommand{
		ChannelID: "chan-a", CommandID: "cmd-1", Action: "group.dial",
		Targets: []string{"only@s.whatsapp.net"},
	}, noFetch); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	failed := pub.typed(call.EventCommandFailed)
	if len(failed) != 1 || failed[0].Error.Code != call.CodeInvalidTarget {
		t.Errorf("failures = %+v, want one invalid_target", failed)
	}
}

func TestDispatchGroupDialTracksTheCall(t *testing.T) {
	pub := &memPublisher{}
	m := newTestManager(t, pub, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	if err := m.Dispatch(context.Background(), caller, amqp.GatewayCallCommand{
		ChannelID: "chan-a", CommandID: "cmd-1", Action: "group.dial",
		Targets: []string{"a@s.whatsapp.net", "b@s.whatsapp.net"},
		GroupID: "120363000000000000@g.us", Video: true,
	}, noFetch); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	if _, ok := m.Get("chan-a", "GRP1"); !ok {
		t.Fatal("group call was not registered")
	}
	if caller.lastOpts.GroupJID != "120363000000000000@g.us" || !caller.lastOpts.Video {
		t.Errorf("group options = %+v, want the bound video group", caller.lastOpts)
	}
	if len(caller.lastTargets) != 2 {
		t.Errorf("targets = %v, want two", caller.lastTargets)
	}
}

func TestDispatchGroupDialByIDTracksTheCall(t *testing.T) {
	pub := &memPublisher{}
	m := newTestManager(t, pub, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	if err := m.Dispatch(context.Background(), caller, amqp.GatewayCallCommand{
		ChannelID: "chan-a", CommandID: "cmd-1", Action: "group.dial_by_id",
		GroupID: "120363000000000000@g.us",
	}, noFetch); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if _, ok := m.Get("chan-a", "GRP2"); !ok {
		t.Fatal("group call was not registered")
	}
	if caller.lastGroupID != "120363000000000000@g.us" {
		t.Errorf("group id = %q, want the group jid", caller.lastGroupID)
	}
}

func TestDispatchGroupDialByIDNeedsAGroup(t *testing.T) {
	pub := &memPublisher{}
	m := newTestManager(t, pub, newMemStore(), time.Now)
	caller := &fakeCaller{}

	if err := m.Dispatch(context.Background(), caller, amqp.GatewayCallCommand{
		ChannelID: "chan-a", CommandID: "cmd-1", Action: "group.dial_by_id",
	}, noFetch); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	failed := pub.typed(call.EventCommandFailed)
	if len(failed) != 1 || failed[0].Error.Code != call.CodeInvalidTarget {
		t.Errorf("failures = %+v, want one invalid_target", failed)
	}
}

func TestDispatchLinkCreatePublishesTheToken(t *testing.T) {
	pub := &memPublisher{}
	m := newTestManager(t, pub, newMemStore(), time.Now)
	caller := &fakeCaller{}

	if err := m.Dispatch(context.Background(), caller, amqp.GatewayCallCommand{
		ChannelID: "chan-a", CommandID: "cmd-1", Action: "link.create", Video: true,
	}, noFetch); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	created := pub.typed(call.EventLinkCreated)
	if len(created) != 1 || created[0].Link == nil || created[0].Link.Token != "TOK" {
		t.Errorf("link.created = %+v, want the TOK link", created)
	}
	if !created[0].Link.Video {
		t.Error("link.created is not marked as a video link")
	}
}

func TestDispatchLinkPreviewPublishesApprovalState(t *testing.T) {
	pub := &memPublisher{}
	m := newTestManager(t, pub, newMemStore(), time.Now)
	caller := &fakeCaller{}

	if err := m.Dispatch(context.Background(), caller, amqp.GatewayCallCommand{
		ChannelID: "chan-a", CommandID: "cmd-1", Action: "link.preview", LinkToken: "TOK",
	}, noFetch); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	preview := pub.typed(call.EventLinkPreview)
	if len(preview) != 1 || preview[0].Link == nil || !preview[0].Link.ApprovalRequired {
		t.Errorf("link.preview = %+v, want approval required", preview)
	}
}

func TestDispatchLinkJoinTracksTheCall(t *testing.T) {
	pub := &memPublisher{}
	m := newTestManager(t, pub, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	if err := m.Dispatch(context.Background(), caller, amqp.GatewayCallCommand{
		ChannelID: "chan-a", CommandID: "cmd-1", Action: "link.join", LinkToken: "TOK",
	}, noFetch); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if _, ok := m.Get("chan-a", "LINK1"); !ok {
		t.Fatal("joined call was not registered")
	}
}

// record:false turns recording off for one call without touching the default.
func TestDispatchDialHonoursRecordFalse(t *testing.T) {
	pub := &memPublisher{}
	m := newTestManager(t, pub, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	off := false
	if err := m.Dispatch(context.Background(), caller, amqp.GatewayCallCommand{
		ChannelID: "chan-a", CommandID: "cmd-1", Action: "dial",
		To: "+5511888888888", Record: &off,
	}, noFetch); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	tracked, ok := m.Get("chan-a", "OUT1")
	if !ok {
		t.Fatal("outbound call was not registered")
	}
	if tracked.Recorder != nil {
		t.Error("call has a recorder despite record:false")
	}
}

func TestDispatchPlayFetchesAndStreamsAudio(t *testing.T) {
	pub := &memPublisher{}
	m := newTestManager(t, pub, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)
	lc := &fakeCall{id: "C1"}
	caller.fireIncoming(lc)

	fetch := func(context.Context, string) ([]byte, error) { return []byte{0x01, 0x02, 0x03, 0x04}, nil }
	if err := m.Dispatch(context.Background(), caller, amqp.GatewayCallCommand{
		ChannelID: "chan-a", CallID: "C1", CommandID: "cmd-1",
		Action: "play", MediaURL: "https://example.test/hello.pcm",
	}, fetch); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	actions := lc.recordedActions()
	if len(actions) != 1 || actions[0] != "play" {
		t.Errorf("actions = %v, want [play]", actions)
	}
}

func TestDispatchPlayReportsAFetchFailure(t *testing.T) {
	pub := &memPublisher{}
	m := newTestManager(t, pub, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)
	caller.fireIncoming(&fakeCall{id: "C1"})

	fetch := func(context.Context, string) ([]byte, error) { return nil, errors.New("404") }
	if err := m.Dispatch(context.Background(), caller, amqp.GatewayCallCommand{
		ChannelID: "chan-a", CallID: "C1", CommandID: "cmd-1",
		Action: "play", MediaURL: "https://example.test/gone.pcm",
	}, fetch); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	failed := pub.typed(call.EventCommandFailed)
	if len(failed) != 1 || failed[0].Error.Code != call.CodeMediaFetch {
		t.Errorf("failures = %+v, want one media_fetch_failed", failed)
	}
}
