package mapper

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

var ErrSkip = errors.New("mapper: inbound message has no mappable user content")

const maxUnwrapDepth = 8

type Downloader interface {
	Download(ctx context.Context, msg whatsmeow.DownloadableMessage) ([]byte, error)
}

type MediaStore interface {
	Put(ctx context.Context, key, mime string, data []byte) error
}

type InboundText struct {
	Body string `json:"body,omitempty"`
}

type InboundMedia struct {
	Key      string `json:"key,omitempty"`
	MimeType string `json:"mimeType,omitempty"`
	Filename string `json:"filename,omitempty"`
	Caption  string `json:"caption,omitempty"`
}

type InboundLocation struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Name      string  `json:"name,omitempty"`
	Address   string  `json:"address,omitempty"`
}

type InboundContactName struct {
	FormattedName string `json:"formatted_name,omitempty"`
	FirstName     string `json:"first_name,omitempty"`
}

type InboundContactPhone struct {
	Phone string `json:"phone,omitempty"`
	WaID  string `json:"wa_id,omitempty"`
	Type  string `json:"type,omitempty"`
}

type InboundContact struct {
	Name   *InboundContactName   `json:"name,omitempty"`
	Phones []InboundContactPhone `json:"phones,omitempty"`
	Vcard  string                `json:"vcard,omitempty"`
}

type InboundReaction struct {
	MessageID string `json:"messageId,omitempty"`
	Emoji     string `json:"emoji,omitempty"`
}

type InboundEvent struct {
	PhoneNumberID     string           `json:"phoneNumberId"`
	From              string           `json:"from"`
	ProfileName       string           `json:"profileName,omitempty"`
	ProviderMessageID string           `json:"providerMessageId"`
	Timestamp         string           `json:"timestamp"`
	Type              string           `json:"type"`
	Text              *InboundText     `json:"text,omitempty"`
	Media             *InboundMedia    `json:"media,omitempty"`
	Location          *InboundLocation `json:"location,omitempty"`
	Contacts          []InboundContact `json:"contacts,omitempty"`
	ContextMessageID  string           `json:"contextMessageId,omitempty"`
	Reaction          *InboundReaction `json:"reaction,omitempty"`
}

type StatusError struct {
	Code   string `json:"code"`
	Reason string `json:"reason,omitempty"`
}

type StatusEvent struct {
	ProviderMessageID string       `json:"providerMessageId"`
	OpaqueMessageID   string       `json:"opaqueMessageId,omitempty"`
	Status            string       `json:"status"`
	Timestamp         string       `json:"timestamp"`
	Error             *StatusError `json:"error,omitempty"`
}

func BuildInbound(ctx context.Context, dl Downloader, s3 MediaStore, channelID, tenantID string, evt *events.Message) (InboundEvent, error) {
	out := InboundEvent{
		PhoneNumberID:     channelID,
		From:              evt.Info.Sender.User,
		ProfileName:       evt.Info.PushName,
		ProviderMessageID: evt.Info.ID,
		Timestamp:         strconv.FormatInt(evt.Info.Timestamp.Unix(), 10),
	}

	msg := unwrapMessage(evt.Message)
	switch {
	case msg.GetConversation() != "":
		out.Type = "text"
		out.Text = &InboundText{Body: msg.GetConversation()}

	case msg.GetExtendedTextMessage() != nil:
		ext := msg.GetExtendedTextMessage()
		out.Type = "text"
		out.Text = &InboundText{Body: ext.GetText()}
		out.ContextMessageID = ext.GetContextInfo().GetStanzaID()

	case msg.GetReactionMessage() != nil:
		r := msg.GetReactionMessage()
		out.Type = "reaction"
		out.Reaction = &InboundReaction{MessageID: r.GetKey().GetID(), Emoji: r.GetText()}

	case msg.GetImageMessage() != nil:
		img := msg.GetImageMessage()
		media, err := downloadAndStore(ctx, dl, s3, tenantID, evt.Info.ID, img.GetMimetype(), img)
		if err != nil {
			return InboundEvent{}, err
		}
		media.Caption = img.GetCaption()
		out.Type = "image"
		out.Media = media
		out.ContextMessageID = img.GetContextInfo().GetStanzaID()

	case msg.GetVideoMessage() != nil:
		video := msg.GetVideoMessage()
		media, err := downloadAndStore(ctx, dl, s3, tenantID, evt.Info.ID, video.GetMimetype(), video)
		if err != nil {
			return InboundEvent{}, err
		}
		media.Caption = video.GetCaption()
		out.Type = "video"
		out.Media = media
		out.ContextMessageID = video.GetContextInfo().GetStanzaID()

	case msg.GetAudioMessage() != nil:
		audio := msg.GetAudioMessage()
		media, err := downloadAndStore(ctx, dl, s3, tenantID, evt.Info.ID, audio.GetMimetype(), audio)
		if err != nil {
			return InboundEvent{}, err
		}
		out.Type = "audio"
		out.Media = media
		out.ContextMessageID = audio.GetContextInfo().GetStanzaID()

	case msg.GetDocumentMessage() != nil:
		doc := msg.GetDocumentMessage()
		media, err := downloadAndStore(ctx, dl, s3, tenantID, evt.Info.ID, doc.GetMimetype(), doc)
		if err != nil {
			return InboundEvent{}, err
		}
		media.Filename = doc.GetFileName()
		media.Caption = doc.GetCaption()
		out.Type = "document"
		out.Media = media
		out.ContextMessageID = doc.GetContextInfo().GetStanzaID()

	case msg.GetLocationMessage() != nil:
		loc := msg.GetLocationMessage()
		out.Type = "location"
		out.Location = &InboundLocation{
			Latitude:  loc.GetDegreesLatitude(),
			Longitude: loc.GetDegreesLongitude(),
			Name:      loc.GetName(),
			Address:   loc.GetAddress(),
		}
		out.ContextMessageID = loc.GetContextInfo().GetStanzaID()

	case msg.GetContactMessage() != nil:
		contact := msg.GetContactMessage()
		out.Type = "contacts"
		out.Contacts = []InboundContact{contactFrom(contact.GetDisplayName(), contact.GetVcard())}
		out.ContextMessageID = contact.GetContextInfo().GetStanzaID()

	case msg.GetContactsArrayMessage() != nil:
		arr := msg.GetContactsArrayMessage()
		out.Type = "contacts"
		contacts := make([]InboundContact, 0, len(arr.GetContacts()))
		for _, c := range arr.GetContacts() {
			contacts = append(contacts, contactFrom(c.GetDisplayName(), c.GetVcard()))
		}
		out.Contacts = contacts
		out.ContextMessageID = arr.GetContextInfo().GetStanzaID()

	default:
		return InboundEvent{}, ErrSkip
	}

	return out, nil
}

func unwrapMessage(msg *waE2E.Message) *waE2E.Message {
	for i := 0; i < maxUnwrapDepth; i++ {
		switch {
		case msg.GetEphemeralMessage().GetMessage() != nil:
			msg = msg.GetEphemeralMessage().GetMessage()
		case msg.GetViewOnceMessage().GetMessage() != nil:
			msg = msg.GetViewOnceMessage().GetMessage()
		case msg.GetViewOnceMessageV2().GetMessage() != nil:
			msg = msg.GetViewOnceMessageV2().GetMessage()
		case msg.GetViewOnceMessageV2Extension().GetMessage() != nil:
			msg = msg.GetViewOnceMessageV2Extension().GetMessage()
		case msg.GetDocumentWithCaptionMessage().GetMessage() != nil:
			msg = msg.GetDocumentWithCaptionMessage().GetMessage()
		default:
			return msg
		}
	}
	return msg
}

func contactFrom(displayName, vcard string) InboundContact {
	return InboundContact{
		Name:  &InboundContactName{FormattedName: displayName},
		Vcard: vcard,
	}
}

func downloadAndStore(ctx context.Context, dl Downloader, s3 MediaStore, tenantID, providerMessageID, mime string, msg whatsmeow.DownloadableMessage) (*InboundMedia, error) {
	data, err := dl.Download(ctx, msg)
	if err != nil {
		return nil, fmt.Errorf("mapper: download media: %w", err)
	}

	key := fmt.Sprintf("inbound-media/%s/%s", tenantID, providerMessageID)
	if err := s3.Put(ctx, key, mime, data); err != nil {
		return nil, fmt.Errorf("mapper: store media: %w", err)
	}

	return &InboundMedia{Key: key, MimeType: mime}, nil
}

func BuildStatus(evt *events.Receipt) []StatusEvent {
	status := statusFor(evt.Type)
	if status == "" {
		return nil
	}

	timestamp := strconv.FormatInt(evt.Timestamp.Unix(), 10)
	out := make([]StatusEvent, 0, len(evt.MessageIDs))
	for _, id := range evt.MessageIDs {
		out = append(out, StatusEvent{ProviderMessageID: id, Status: status, Timestamp: timestamp})
	}
	return out
}

func statusFor(t types.ReceiptType) string {
	switch t {
	case types.ReceiptTypeDelivered:
		return "delivered"
	case types.ReceiptTypeRead:
		return "read"
	default:
		return ""
	}
}
