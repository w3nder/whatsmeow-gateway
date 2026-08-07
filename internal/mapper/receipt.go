package mapper

import (
	"strconv"

	"go.mau.fi/whatsmeow/types/events"
)

type GroupStatusEvent struct {
	TenantID       string   `json:"tenantId"`
	ChannelID      string   `json:"channelId"`
	GroupJID       string   `json:"groupJid"`
	ParticipantJID string   `json:"participantJid"`
	MessageIDs     []string `json:"messageIds"`
	Status         string   `json:"status"`
	Timestamp      string   `json:"timestamp"`
}

type GroupStatusDeps struct {
	ChannelID string
	TenantID  string
}

func BuildGroupStatus(deps GroupStatusDeps, evt *events.Receipt) *GroupStatusEvent {
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
		TenantID:       deps.TenantID,
		ChannelID:      deps.ChannelID,
		GroupJID:       evt.Chat.String(),
		ParticipantJID: evt.Sender.ToNonAD().String(),
		MessageIDs:     evt.MessageIDs,
		Status:         status,
		Timestamp:      strconv.FormatInt(evt.Timestamp.Unix(), 10),
	}
}
