package gateway

import (
	"testing"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func carryingHistory(fromMe bool, notif *waE2E.HistorySyncNotification) *events.Message {
	return &events.Message{
		Info: types.MessageInfo{MessageSource: types.MessageSource{IsFromMe: fromMe}},
		Message: &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{
			Type:                    waE2E.ProtocolMessage_HISTORY_SYNC_NOTIFICATION.Enum(),
			HistorySyncNotification: notif,
		}},
	}
}

func TestHistoryNotificationIsTakenFromTheOwnDevice(t *testing.T) {
	notif := &waE2E.HistorySyncNotification{DirectPath: proto.String("/v/t62.history/chunk")}
	if got := historyNotification(carryingHistory(true, notif)); got != notif {
		t.Fatalf("the own device's notification must be taken, got %v", got)
	}
}

func TestHistoryNotificationFromAContactIsNeverTaken(t *testing.T) {
	notif := &waE2E.HistorySyncNotification{DirectPath: proto.String("/v/t62.history/forged")}
	if got := historyNotification(carryingHistory(false, notif)); got != nil {
		t.Fatalf("a contact must never be able to inject history, got %v", got)
	}
}

func TestHistoryNotificationIgnoresOrdinaryMessages(t *testing.T) {
	plain := &events.Message{
		Info:    types.MessageInfo{MessageSource: types.MessageSource{IsFromMe: true}},
		Message: &waE2E.Message{Conversation: proto.String("oi")},
	}
	if got := historyNotification(plain); got != nil {
		t.Fatalf("an ordinary message is not a history notification, got %v", got)
	}
}
