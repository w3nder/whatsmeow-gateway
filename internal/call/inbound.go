package call

import (
	"context"

	"go.mau.fi/whatsmeow/types"

	"github.com/w3nder/whatsmeow-gateway/internal/avatar"
	"github.com/w3nder/whatsmeow-gateway/internal/senderid"
)

// AvatarSource resolves the profile photo of the peer a call is with. The
// gateway binds it to a channel and tenant before handing it over, the same
// way mapper.AvatarSource is bound for a message, so both event kinds name
// the same photo for the same contact. A nil source leaves calls without one.
type AvatarSource interface {
	For(ctx context.Context, jid types.JID) *avatar.Picture
}

// InboundCallEvent mirrors mapper.InboundEvent's shape for a call, so the
// backend's existing message pipeline treats it as any other inbound message.
//
// It cannot simply be mapper.InboundEvent: mapper.BuildInbound builds that
// type from a *events.Message straight off whatsmeow, and a call has no such
// message -- there is no protobuf payload to unwrap, only the call metadata
// this package already tracks.
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

// InboundCallRich is the richContent payload for a type: "call" inbound
// event. State starts at "ringing": the chat row is created the moment the
// call arrives, before any answer/reject decision exists to report.
type InboundCallRich struct {
	Kind      string `json:"kind"`
	Direction string `json:"direction"`
	IsVideo   bool   `json:"isVideo,omitempty"`
	State     string `json:"state"`
}

// NewInboundCallEvent builds the inbound-message-shaped view of a call, arriving
// or placed. ProviderMessageID is the call id, not a message id, so the backend
// can correlate later call.v1 lifecycle events with the chat row this event
// creates.
//
// senderLid and senderPn are the peer's identity, already resolved by
// senderid.Resolve the same way an inbound message's sender is -- this event
// and the call.Event published on the call routing key must agree on who the
// call was with, or the chat row this creates and the lifecycle events that
// update it point the backend at two different contacts. That holds
// regardless of fromMe: exactly as mapper.BuildInbound keys a from-me message
// to the chat contact rather than to our own device, these fields always
// describe the person the call is with, never us.
//
// State always starts "ringing": whichever side placed the call, the chat row
// is created before an answer/reject decision exists to report.
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
