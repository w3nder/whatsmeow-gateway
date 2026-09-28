package mapper

import (
	"strconv"
	"time"

	"go.mau.fi/whatsmeow/types"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
)

type ResolvedRecipient struct {
	Phone string `json:"phone"`
	Lid   string `json:"lid"`
}

func SentStatus(cmd amqp.GatewaySendCommand, chat types.JID, providerMessageID string, sentAt time.Time) StatusEvent {
	evt := StatusEvent{
		ProviderMessageID: providerMessageID,
		OpaqueMessageID:   cmd.MessageID,
		Status:            "sent",
		Timestamp:         strconv.FormatInt(sentAt.Unix(), 10),
	}
	if recipient := resolvedRecipient(cmd.To, chat); recipient != nil {
		evt.TenantID = cmd.TenantID
		evt.ResolvedRecipient = recipient
	}
	return evt
}

func resolvedRecipient(requested string, chat types.JID) *ResolvedRecipient {
	pn, err := types.ParseJID(requested)
	if err != nil || pn.Server != types.DefaultUserServer || chat.Server != types.HiddenUserServer {
		return nil
	}
	return &ResolvedRecipient{Phone: pn.User, Lid: chat.User}
}
