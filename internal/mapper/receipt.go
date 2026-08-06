package mapper

import (
	"strconv"

	"go.mau.fi/whatsmeow/types/events"
)

type GroupStatusEvent struct {
	GroupJID       string   `json:"groupJid"`
	ParticipantJID string   `json:"participantJid"`
	MessageIDs     []string `json:"messageIds"`
	Status         string   `json:"status"`
	Timestamp      string   `json:"timestamp"`
}

func BuildGroupStatus(evt *events.Receipt) *GroupStatusEvent {
	if KindOf(evt.Chat) != ChatGroup {
		return nil
	}

	status := statusFor(evt.Type)
	if status == "" {
		return nil
	}

	if len(evt.MessageIDs) == 0 {
		return nil
	}

	return &GroupStatusEvent{
		GroupJID:       evt.Chat.String(),
		ParticipantJID: evt.Sender.ToNonAD().String(),
		MessageIDs:     evt.MessageIDs,
		Status:         status,
		Timestamp:      strconv.FormatInt(evt.Timestamp.Unix(), 10),
	}
}
