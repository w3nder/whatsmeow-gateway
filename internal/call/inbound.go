package call

import (
	"context"

	"go.mau.fi/whatsmeow/types"

	"github.com/w3nder/whatsmeow-gateway/internal/avatar"
	"github.com/w3nder/whatsmeow-gateway/internal/senderid"
)

type AvatarSource interface {
	For(ctx context.Context, jid types.JID) *avatar.Picture
}

type InboundCallEvent struct {
	PhoneNumberID     string           `json:"phoneNumberId"`
	TenantID          string           `json:"tenantId"`
	ChannelID         string           `json:"channelId"`
	From              string           `json:"from"`
	SenderLid         string           `json:"senderLid,omitempty"`
	SenderPn          string           `json:"senderPn,omitempty"`
	FromMe            bool             `json:"fromMe,omitempty"`
	ProviderMessageID string           `json:"providerMessageId"`
	Timestamp         string           `json:"timestamp"`
	Type              string           `json:"type"`
	RichContent       *InboundCallRich `json:"richContent,omitempty"`
	ProfilePicture    *avatar.Picture  `json:"profilePicture,omitempty"`
}

type InboundCallRich struct {
	Kind      string `json:"kind"`
	Direction string `json:"direction"`
	IsVideo   bool   `json:"isVideo,omitempty"`
	State     string `json:"state"`
}

func NewInboundCallEvent(id Identity, channelID, callID, senderLid, senderPn, direction string, fromMe, isVideo bool, timestamp string, picture *avatar.Picture) InboundCallEvent {
	return InboundCallEvent{
		PhoneNumberID:     id.PhoneNumberID,
		TenantID:          id.TenantID,
		ChannelID:         channelID,
		From:              senderid.From(senderLid, senderPn),
		SenderLid:         senderLid,
		SenderPn:          senderPn,
		FromMe:            fromMe,
		ProviderMessageID: callID,
		Timestamp:         timestamp,
		Type:              "call",
		RichContent: &InboundCallRich{
			Kind:      "call",
			Direction: direction,
			IsVideo:   isVideo,
			State:     "ringing",
		},
		ProfilePicture: picture,
	}
}
