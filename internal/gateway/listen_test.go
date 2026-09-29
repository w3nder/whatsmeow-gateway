package gateway

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/w3nder/whatsmeow-gateway/internal/channelsettings"
	"github.com/w3nder/whatsmeow-gateway/internal/session"
)

var (
	testGroupJID  = types.NewJID("120363000000000001", types.GroupServer)
	testMemberJID = types.NewJID("5511777776666", types.DefaultUserServer)
)

func groupMessage() *events.Message {
	return &events.Message{Info: types.MessageInfo{
		MessageSource: types.MessageSource{Chat: testGroupJID, Sender: testMemberJID},
		ID:            "GROUPMSG1",
		Timestamp:     time.Unix(1754300000, 0),
	}}
}

func privateMessage() *events.Message {
	return &events.Message{Info: types.MessageInfo{
		MessageSource: types.MessageSource{Chat: testMemberJID, Sender: testMemberJID},
		ID:            "PRIVATEMSG1",
		Timestamp:     time.Unix(1754300000, 0),
	}}
}

func groupReceipt() *events.Receipt {
	return &events.Receipt{MessageSource: types.MessageSource{Chat: testGroupJID, Sender: testMemberJID}}
}

func groupInfoEvent() *events.GroupInfo {
	return &events.GroupInfo{JID: testGroupJID}
}

func TestIsGroupEventRecognisesOnlyGroupChats(t *testing.T) {
	for _, tc := range []struct {
		name string
		evt  any
		want bool
	}{
		{"group message", groupMessage(), true},
		{"group receipt", groupReceipt(), true},
		{"group info", groupInfoEvent(), true},
		{"private message", privateMessage(), false},
		{"private receipt", &events.Receipt{MessageSource: types.MessageSource{Chat: testMemberJID}}, false},
		{"connected", &events.Connected{}, false},
		{"logged out", &events.LoggedOut{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isGroupEvent(tc.evt); got != tc.want {
				t.Errorf("isGroupEvent = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIgnoredByListenGroupsFollowsTheChannelOption(t *testing.T) {
	settings := channelsettings.NewMap()
	settings.Set("muted", channelsettings.Settings{ListenGroups: false, ReceiveCalls: true})
	g := &gateway{settings: settings}

	if !g.ignoredByListenGroups("muted", groupMessage()) {
		t.Error("a group message on a channel that does not listen to groups must be ignored")
	}
	if !g.ignoredByListenGroups("muted", groupReceipt()) {
		t.Error("a group receipt on a channel that does not listen to groups must be ignored")
	}
	if !g.ignoredByListenGroups("muted", groupInfoEvent()) {
		t.Error("a group info event on a channel that does not listen to groups must be ignored")
	}
	if g.ignoredByListenGroups("muted", privateMessage()) {
		t.Error("a private message must never be ignored by the group option")
	}
	if g.ignoredByListenGroups("listening", groupMessage()) {
		t.Error("a channel with no stored option listens to groups by default")
	}
}

func newListenTestGateway(logs *bytes.Buffer, settings *channelsettings.Map) *gateway {
	return &gateway{
		settings: settings,
		logger:   slog.New(slog.NewTextHandler(logs, nil)),
		manager: session.NewManager(func(string, *types.JID) (session.WAClient, error) {
			return nil, errors.New("no client in this test")
		}),
		tenantByChannel: map[string]string{},
	}
}

func TestHandleSessionEventDropsGroupEventsBeforeMappingWhenTheChannelDoesNotListen(t *testing.T) {
	var logs bytes.Buffer
	settings := channelsettings.NewMap()
	settings.Set("channel-1", channelsettings.Settings{ListenGroups: false, ReceiveCalls: true})
	g := newListenTestGateway(&logs, settings)

	g.handleSessionEvent("channel-1", groupMessage())
	g.handleSessionEvent("channel-1", groupReceipt())
	g.handleSessionEvent("channel-1", groupInfoEvent())

	if strings.Contains(logs.String(), "inbound event received") {
		t.Fatalf("the group message reached the inbound mapping:\n%s", logs.String())
	}
}

func TestHandleSessionEventKeepsGroupMessagesWhenTheChannelListens(t *testing.T) {
	var logs bytes.Buffer
	g := newListenTestGateway(&logs, channelsettings.NewMap())

	g.handleSessionEvent("channel-1", groupMessage())

	if !strings.Contains(logs.String(), "inbound event received") {
		t.Fatalf("a listening channel must process the group message, logs:\n%s", logs.String())
	}
}

func TestHandleSessionEventKeepsPrivateMessagesOnAChannelThatDoesNotListenToGroups(t *testing.T) {
	var logs bytes.Buffer
	settings := channelsettings.NewMap()
	settings.Set("channel-1", channelsettings.Settings{ListenGroups: false, ReceiveCalls: true})
	g := newListenTestGateway(&logs, settings)

	g.handleSessionEvent("channel-1", privateMessage())

	if !strings.Contains(logs.String(), "inbound event received") {
		t.Fatalf("private messages must keep flowing, logs:\n%s", logs.String())
	}
}
