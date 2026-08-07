package mapper_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/mapper"
)

var stubUploadResponse = whatsmeow.UploadResponse{
	URL:           "https://mmg.whatsapp.net/stub",
	DirectPath:    "/v/stub",
	MediaKey:      []byte("media-key"),
	FileEncSHA256: []byte("enc-sha"),
	FileSHA256:    []byte("sha"),
	FileLength:    1234,
}

type stubUploader struct {
	resp whatsmeow.UploadResponse
	err  error
}

func (u stubUploader) Upload(ctx context.Context, data []byte, mt whatsmeow.MediaType) (whatsmeow.UploadResponse, error) {
	return u.resp, u.err
}

func (u stubUploader) BuildEdit(chat types.JID, id types.MessageID, newContent *waE2E.Message) *waE2E.Message {
	return &waE2E.Message{Conversation: proto.String("EDIT|" + chat.String() + "|" + string(id) + "|" + newContent.GetConversation())}
}

func (u stubUploader) BuildRevoke(chat, sender types.JID, id types.MessageID) *waE2E.Message {
	return &waE2E.Message{Conversation: proto.String("REVOKE|" + chat.String() + "|sender=" + sender.String() + "|" + string(id))}
}

func (u stubUploader) BuildReaction(chat, sender types.JID, id types.MessageID, reaction string) *waE2E.Message {
	return &waE2E.Message{Conversation: proto.String("REACTION|" + chat.String() + "|sender=" + sender.String() + "|" + string(id) + "|" + reaction)}
}

func stubFetch(data []byte, err error) mapper.MediaFetcher {
	return func(ctx context.Context, url string) ([]byte, error) {
		return data, err
	}
}

func TestBuildOutboundTextAddressesByPhoneJID(t *testing.T) {
	cmd := amqp.GatewaySendCommand{To: "5511999@s.whatsapp.net", Type: "text", Text: "hello"}

	to, msg, _, err := mapper.BuildOutbound(context.Background(), stubUploader{}, cmd, stubFetch(nil, nil))
	if err != nil {
		t.Fatalf("BuildOutbound: %v", err)
	}
	if to.Server != types.DefaultUserServer || to.User != "5511999" {
		t.Fatalf("unexpected JID: %v", to)
	}
	if msg.GetConversation() != "hello" {
		t.Fatalf("expected Conversation=hello, got %q", msg.GetConversation())
	}
}

func TestBuildOutboundTextAddressesByLIDJID(t *testing.T) {
	cmd := amqp.GatewaySendCommand{To: "173907587899617@lid", Type: "text", Text: "hello"}

	to, msg, _, err := mapper.BuildOutbound(context.Background(), stubUploader{}, cmd, stubFetch(nil, nil))
	if err != nil {
		t.Fatalf("BuildOutbound: %v", err)
	}
	if to.Server != types.HiddenUserServer || to.User != "173907587899617" {
		t.Fatalf("unexpected JID: %v", to)
	}
	if msg.GetConversation() != "hello" {
		t.Fatalf("expected Conversation=hello, got %q", msg.GetConversation())
	}
}

func TestBuildOutboundRejectsBareNumberRecipient(t *testing.T) {
	cmd := amqp.GatewaySendCommand{To: "5511999999999", Type: "text", Text: "hello"}

	_, _, _, err := mapper.BuildOutbound(context.Background(), stubUploader{}, cmd, stubFetch(nil, nil))
	if err == nil {
		t.Fatal("expected error for bare number recipient without JID server")
	}
}

func TestBuildOutboundTextReply(t *testing.T) {
	cmd := amqp.GatewaySendCommand{
		To:   "5511999999999@s.whatsapp.net",
		Type: "text",
		Text: "hello again",
		ReplyTo: &amqp.ReplyToPayload{
			ProviderMessageID: "wamid.stub",
			ParticipantJID:    "5511888888888@s.whatsapp.net",
		},
	}

	_, msg, _, err := mapper.BuildOutbound(context.Background(), stubUploader{}, cmd, stubFetch(nil, nil))
	if err != nil {
		t.Fatalf("BuildOutbound: %v", err)
	}
	ext := msg.GetExtendedTextMessage()
	if ext == nil {
		t.Fatalf("expected ExtendedTextMessage, got %+v", msg)
	}
	if ext.GetText() != "hello again" {
		t.Fatalf("expected Text=hello again, got %q", ext.GetText())
	}
	ctxInfo := ext.GetContextInfo()
	if ctxInfo.GetStanzaID() != "wamid.stub" {
		t.Fatalf("expected StanzaID=wamid.stub, got %q", ctxInfo.GetStanzaID())
	}
	if ctxInfo.GetParticipant() != "5511888888888@s.whatsapp.net" {
		t.Fatalf("expected Participant, got %q", ctxInfo.GetParticipant())
	}
}

func TestBuildOutboundImage(t *testing.T) {
	cmd := amqp.GatewaySendCommand{
		To:   "5511999999999@s.whatsapp.net",
		Type: "image",
		Text: "a caption",
		Media: &amqp.MediaPayload{
			URL:  "https://s3.example/media.jpg",
			Mime: "image/jpeg",
		},
	}

	_, msg, _, err := mapper.BuildOutbound(context.Background(), stubUploader{resp: stubUploadResponse}, cmd, stubFetch([]byte("bytes"), nil))
	if err != nil {
		t.Fatalf("BuildOutbound: %v", err)
	}
	img := msg.GetImageMessage()
	if img == nil {
		t.Fatalf("expected ImageMessage, got %+v", msg)
	}
	if img.GetURL() != stubUploadResponse.URL || img.GetDirectPath() != stubUploadResponse.DirectPath {
		t.Fatalf("expected URL/DirectPath from upload response, got %+v", img)
	}
	if string(img.GetMediaKey()) != "media-key" || string(img.GetFileEncSHA256()) != "enc-sha" || string(img.GetFileSHA256()) != "sha" {
		t.Fatalf("expected media key/hashes from upload response, got %+v", img)
	}
	if img.GetFileLength() != stubUploadResponse.FileLength {
		t.Fatalf("expected FileLength=%d, got %d", stubUploadResponse.FileLength, img.GetFileLength())
	}
	if img.GetMimetype() != "image/jpeg" {
		t.Fatalf("expected Mimetype=image/jpeg, got %q", img.GetMimetype())
	}
	if img.GetCaption() != "a caption" {
		t.Fatalf("expected Caption='a caption', got %q", img.GetCaption())
	}
}

func TestBuildOutboundImageReply(t *testing.T) {
	cmd := amqp.GatewaySendCommand{
		To:   "5511999999999@s.whatsapp.net",
		Type: "image",
		Media: &amqp.MediaPayload{
			URL:  "https://s3.example/media.jpg",
			Mime: "image/jpeg",
		},
		ReplyTo: &amqp.ReplyToPayload{
			ProviderMessageID: "wamid.quoted-image",
			ParticipantJID:    "5511888888888@s.whatsapp.net",
		},
	}

	_, msg, _, err := mapper.BuildOutbound(context.Background(), stubUploader{resp: stubUploadResponse}, cmd, stubFetch([]byte("bytes"), nil))
	if err != nil {
		t.Fatalf("BuildOutbound: %v", err)
	}
	img := msg.GetImageMessage()
	if img == nil {
		t.Fatalf("expected ImageMessage, got %+v", msg)
	}
	if img.GetContextInfo().GetStanzaID() != "wamid.quoted-image" {
		t.Fatalf("expected ContextInfo.StanzaID=wamid.quoted-image, got %q", img.GetContextInfo().GetStanzaID())
	}
	if img.GetContextInfo().GetParticipant() != "5511888888888@s.whatsapp.net" {
		t.Fatalf("expected ContextInfo.Participant, got %q", img.GetContextInfo().GetParticipant())
	}
}

func TestBuildOutboundVideo(t *testing.T) {
	cmd := amqp.GatewaySendCommand{
		To:   "5511999999999@s.whatsapp.net",
		Type: "video",
		Text: "a video caption",
		Media: &amqp.MediaPayload{
			URL:  "https://s3.example/media.mp4",
			Mime: "video/mp4",
		},
	}

	_, msg, _, err := mapper.BuildOutbound(context.Background(), stubUploader{resp: stubUploadResponse}, cmd, stubFetch([]byte("bytes"), nil))
	if err != nil {
		t.Fatalf("BuildOutbound: %v", err)
	}
	video := msg.GetVideoMessage()
	if video == nil {
		t.Fatalf("expected VideoMessage, got %+v", msg)
	}
	if video.GetURL() != stubUploadResponse.URL {
		t.Fatalf("expected URL from upload response, got %q", video.GetURL())
	}
	if video.GetMimetype() != "video/mp4" {
		t.Fatalf("expected Mimetype=video/mp4, got %q", video.GetMimetype())
	}
	if video.GetCaption() != "a video caption" {
		t.Fatalf("expected Caption, got %q", video.GetCaption())
	}
	if video.GetFileLength() != stubUploadResponse.FileLength {
		t.Fatalf("expected FileLength=%d, got %d", stubUploadResponse.FileLength, video.GetFileLength())
	}
}

func TestBuildOutboundAudio(t *testing.T) {
	cmd := amqp.GatewaySendCommand{
		To:   "5511999999999@s.whatsapp.net",
		Type: "audio",
		Media: &amqp.MediaPayload{
			URL:  "https://s3.example/media.ogg",
			Mime: "audio/ogg",
		},
	}

	_, msg, _, err := mapper.BuildOutbound(context.Background(), stubUploader{resp: stubUploadResponse}, cmd, stubFetch([]byte("bytes"), nil))
	if err != nil {
		t.Fatalf("BuildOutbound: %v", err)
	}
	audio := msg.GetAudioMessage()
	if audio == nil {
		t.Fatalf("expected AudioMessage, got %+v", msg)
	}
	if audio.GetURL() != stubUploadResponse.URL {
		t.Fatalf("expected URL from upload response, got %q", audio.GetURL())
	}
	if audio.GetMimetype() != "audio/ogg" {
		t.Fatalf("expected Mimetype=audio/ogg, got %q", audio.GetMimetype())
	}
	if audio.GetFileLength() != stubUploadResponse.FileLength {
		t.Fatalf("expected FileLength=%d, got %d", stubUploadResponse.FileLength, audio.GetFileLength())
	}
}

func TestBuildOutboundDocument(t *testing.T) {
	cmd := amqp.GatewaySendCommand{
		To:   "5511999999999@s.whatsapp.net",
		Type: "document",
		Text: "doc caption",
		Media: &amqp.MediaPayload{
			URL:      "https://s3.example/media.pdf",
			Mime:     "application/pdf",
			Filename: "invoice.pdf",
		},
	}

	_, msg, _, err := mapper.BuildOutbound(context.Background(), stubUploader{resp: stubUploadResponse}, cmd, stubFetch([]byte("bytes"), nil))
	if err != nil {
		t.Fatalf("BuildOutbound: %v", err)
	}
	doc := msg.GetDocumentMessage()
	if doc == nil {
		t.Fatalf("expected DocumentMessage, got %+v", msg)
	}
	if doc.GetFileName() != "invoice.pdf" {
		t.Fatalf("expected FileName=invoice.pdf, got %q", doc.GetFileName())
	}
	if doc.GetMimetype() != "application/pdf" {
		t.Fatalf("expected Mimetype=application/pdf, got %q", doc.GetMimetype())
	}
	if doc.GetCaption() != "doc caption" {
		t.Fatalf("expected Caption, got %q", doc.GetCaption())
	}
	if doc.GetFileLength() != stubUploadResponse.FileLength {
		t.Fatalf("expected FileLength=%d, got %d", stubUploadResponse.FileLength, doc.GetFileLength())
	}
}

func TestBuildOutboundMediaFetchError(t *testing.T) {
	cmd := amqp.GatewaySendCommand{
		To:    "5511999999999@s.whatsapp.net",
		Type:  "image",
		Media: &amqp.MediaPayload{URL: "https://s3.example/media.jpg", Mime: "image/jpeg"},
	}

	fetchErr := errors.New("boom")
	_, _, _, err := mapper.BuildOutbound(context.Background(), stubUploader{}, cmd, stubFetch(nil, fetchErr))
	if err == nil {
		t.Fatal("expected error when media fetch fails")
	}
}

func TestBuildOutboundMediaUploadError(t *testing.T) {
	cmd := amqp.GatewaySendCommand{
		To:    "5511999999999@s.whatsapp.net",
		Type:  "image",
		Media: &amqp.MediaPayload{URL: "https://s3.example/media.jpg", Mime: "image/jpeg"},
	}

	uploadErr := errors.New("upload failed")
	_, _, _, err := mapper.BuildOutbound(context.Background(), stubUploader{err: uploadErr}, cmd, stubFetch([]byte("bytes"), nil))
	if err == nil {
		t.Fatal("expected error when upload fails")
	}
}

func TestBuildOutboundMediaMissingPayload(t *testing.T) {
	cmd := amqp.GatewaySendCommand{To: "5511999999999@s.whatsapp.net", Type: "image"}

	_, _, _, err := mapper.BuildOutbound(context.Background(), stubUploader{}, cmd, stubFetch(nil, nil))
	if err == nil {
		t.Fatal("expected error when media payload is missing")
	}
}

func TestBuildOutboundLocation(t *testing.T) {
	cmd := amqp.GatewaySendCommand{
		To:   "5511999999999@s.whatsapp.net",
		Type: "location",
		Location: &amqp.LocationPayload{
			Lat:     -23.55052,
			Lng:     -46.633308,
			Name:    "Sender HQ",
			Address: "Av. Paulista, 1000",
		},
	}

	_, msg, _, err := mapper.BuildOutbound(context.Background(), stubUploader{}, cmd, stubFetch(nil, nil))
	if err != nil {
		t.Fatalf("BuildOutbound: %v", err)
	}
	loc := msg.GetLocationMessage()
	if loc == nil {
		t.Fatalf("expected LocationMessage, got %+v", msg)
	}
	if loc.GetDegreesLatitude() != -23.55052 || loc.GetDegreesLongitude() != -46.633308 {
		t.Fatalf("expected lat/lng, got %+v", loc)
	}
	if loc.GetName() != "Sender HQ" || loc.GetAddress() != "Av. Paulista, 1000" {
		t.Fatalf("expected name/address, got %+v", loc)
	}
}

func TestBuildOutboundLocationReply(t *testing.T) {
	cmd := amqp.GatewaySendCommand{
		To:   "5511999999999@s.whatsapp.net",
		Type: "location",
		Location: &amqp.LocationPayload{
			Lat: -23.55052,
			Lng: -46.633308,
		},
		ReplyTo: &amqp.ReplyToPayload{ProviderMessageID: "wamid.quoted-location"},
	}

	_, msg, _, err := mapper.BuildOutbound(context.Background(), stubUploader{}, cmd, stubFetch(nil, nil))
	if err != nil {
		t.Fatalf("BuildOutbound: %v", err)
	}
	loc := msg.GetLocationMessage()
	if loc == nil {
		t.Fatalf("expected LocationMessage, got %+v", msg)
	}
	if loc.GetContextInfo().GetStanzaID() != "wamid.quoted-location" {
		t.Fatalf("expected ContextInfo.StanzaID=wamid.quoted-location, got %q", loc.GetContextInfo().GetStanzaID())
	}
}

func TestBuildOutboundLocationMissingPayload(t *testing.T) {
	cmd := amqp.GatewaySendCommand{To: "5511999999999@s.whatsapp.net", Type: "location"}

	_, _, _, err := mapper.BuildOutbound(context.Background(), stubUploader{}, cmd, stubFetch(nil, nil))
	if err == nil {
		t.Fatal("expected error when location payload is missing")
	}
}

func TestBuildOutboundSingleContact(t *testing.T) {
	cmd := amqp.GatewaySendCommand{
		To:   "5511999999999@s.whatsapp.net",
		Type: "contacts",
		Contacts: []amqp.ContactPayload{
			{Name: "Jane Doe", Vcard: "BEGIN:VCARD\nVERSION:3.0\nFN:Jane Doe\nEND:VCARD"},
		},
	}

	_, msg, _, err := mapper.BuildOutbound(context.Background(), stubUploader{}, cmd, stubFetch(nil, nil))
	if err != nil {
		t.Fatalf("BuildOutbound: %v", err)
	}
	contact := msg.GetContactMessage()
	if contact == nil {
		t.Fatalf("expected ContactMessage, got %+v", msg)
	}
	if contact.GetDisplayName() != "Jane Doe" {
		t.Fatalf("expected DisplayName=Jane Doe, got %q", contact.GetDisplayName())
	}
	if contact.GetVcard() == "" {
		t.Fatalf("expected non-empty Vcard")
	}
}

func TestBuildOutboundMultipleContacts(t *testing.T) {
	cmd := amqp.GatewaySendCommand{
		To:   "5511999999999@s.whatsapp.net",
		Type: "contacts",
		Contacts: []amqp.ContactPayload{
			{Name: "Jane Doe", Vcard: "BEGIN:VCARD\nFN:Jane Doe\nEND:VCARD"},
			{Name: "John Roe", Vcard: "BEGIN:VCARD\nFN:John Roe\nEND:VCARD"},
		},
	}

	_, msg, _, err := mapper.BuildOutbound(context.Background(), stubUploader{}, cmd, stubFetch(nil, nil))
	if err != nil {
		t.Fatalf("BuildOutbound: %v", err)
	}
	arr := msg.GetContactsArrayMessage()
	if arr == nil {
		t.Fatalf("expected ContactsArrayMessage, got %+v", msg)
	}
	if len(arr.GetContacts()) != 2 {
		t.Fatalf("expected 2 contacts, got %d", len(arr.GetContacts()))
	}
	if arr.GetContacts()[0].GetDisplayName() != "Jane Doe" || arr.GetContacts()[1].GetDisplayName() != "John Roe" {
		t.Fatalf("expected contact display names in order, got %+v", arr.GetContacts())
	}
}

func TestBuildOutboundContactsMissingPayload(t *testing.T) {
	cmd := amqp.GatewaySendCommand{To: "5511999999999@s.whatsapp.net", Type: "contacts"}

	_, _, _, err := mapper.BuildOutbound(context.Background(), stubUploader{}, cmd, stubFetch(nil, nil))
	if err == nil {
		t.Fatal("expected error when contacts payload is missing")
	}
}

func TestBuildOutboundUnknownType(t *testing.T) {
	cmd := amqp.GatewaySendCommand{To: "5511999999999@s.whatsapp.net", Type: "sticker"}

	_, _, _, err := mapper.BuildOutbound(context.Background(), stubUploader{}, cmd, stubFetch(nil, nil))
	if err == nil {
		t.Fatal("expected error for unknown type")
	}
}

func TestBuildOutboundEditCallsBuildEdit(t *testing.T) {
	cmd := amqp.GatewaySendCommand{To: "5511999@s.whatsapp.net", Kind: "edit", TargetProviderMessageID: "3EB0ABC", Text: "corrigido"}
	to, msg, _, err := mapper.BuildOutbound(context.Background(), stubUploader{}, cmd, stubFetch(nil, nil))
	if err != nil {
		t.Fatalf("BuildOutbound: %v", err)
	}
	if to.User != "5511999" {
		t.Fatalf("unexpected to: %+v", to)
	}
	got := msg.GetConversation()
	if !strings.HasPrefix(got, "EDIT|") || !strings.Contains(got, "|3EB0ABC|corrigido") {
		t.Fatalf("expected edit routed to BuildEdit, got %q", got)
	}
}

func TestBuildOutboundEditRequiresTarget(t *testing.T) {
	cmd := amqp.GatewaySendCommand{To: "5511999@s.whatsapp.net", Kind: "edit", Text: "x"}
	if _, _, _, err := mapper.BuildOutbound(context.Background(), stubUploader{}, cmd, stubFetch(nil, nil)); err == nil {
		t.Fatalf("expected error for edit without targetProviderMessageId")
	}
}

func TestBuildOutboundRevokeCallsBuildRevoke(t *testing.T) {
	cmd := amqp.GatewaySendCommand{To: "5511999@s.whatsapp.net", Kind: "revoke", TargetProviderMessageID: "3EB0XYZ"}
	_, msg, _, err := mapper.BuildOutbound(context.Background(), stubUploader{}, cmd, stubFetch(nil, nil))
	if err != nil {
		t.Fatalf("BuildOutbound: %v", err)
	}
	got := msg.GetConversation()
	if !strings.HasPrefix(got, "REVOKE|") || !strings.Contains(got, "3EB0XYZ") {
		t.Fatalf("expected revoke routed to BuildRevoke, got %q", got)
	}
}

func TestBuildOutboundReactionToOwnMessageUsesEmptySender(t *testing.T) {
	cmd := amqp.GatewaySendCommand{To: "5511999@s.whatsapp.net", Kind: "reaction", TargetProviderMessageID: "3EB0OWN", TargetFromMe: true, Emoji: "👍"}
	_, msg, _, err := mapper.BuildOutbound(context.Background(), stubUploader{}, cmd, stubFetch(nil, nil))
	if err != nil {
		t.Fatalf("BuildOutbound: %v", err)
	}
	got := msg.GetConversation()
	if !strings.HasPrefix(got, "REACTION|") || !strings.Contains(got, "sender=") || !strings.Contains(got, "3EB0OWN") || !strings.Contains(got, "👍") {
		t.Fatalf("unexpected reaction payload: %q", got)
	}
	if !strings.Contains(got, "sender=|") {
		t.Fatalf("expected empty sender for own-message reaction, got %q", got)
	}
}

func TestBuildOutboundReactionToContactMessageUsesChatSender(t *testing.T) {
	cmd := amqp.GatewaySendCommand{To: "5511999@s.whatsapp.net", Kind: "reaction", TargetProviderMessageID: "3EB0THEM", TargetFromMe: false, Emoji: "❤️"}
	_, msg, _, err := mapper.BuildOutbound(context.Background(), stubUploader{}, cmd, stubFetch(nil, nil))
	if err != nil {
		t.Fatalf("BuildOutbound: %v", err)
	}
	got := msg.GetConversation()
	if !strings.Contains(got, "sender=5511999@s.whatsapp.net") {
		t.Fatalf("expected the chat JID as sender for a contact-message reaction, got %q", got)
	}
}

func TestBuildOutboundReactionRequiresTarget(t *testing.T) {
	cmd := amqp.GatewaySendCommand{To: "5511999@s.whatsapp.net", Kind: "reaction", Emoji: "👍"}
	_, _, _, err := mapper.BuildOutbound(context.Background(), stubUploader{}, cmd, stubFetch(nil, nil))
	if err == nil {
		t.Fatalf("expected an error when reaction has no targetProviderMessageId")
	}
}

func TestBuildOutboundForwardedTextMarksContext(t *testing.T) {
	cmd := amqp.GatewaySendCommand{To: "5511999@s.whatsapp.net", Type: "text", Text: "encaminhada", Forwarded: true}
	_, msg, _, err := mapper.BuildOutbound(context.Background(), stubUploader{}, cmd, stubFetch(nil, nil))
	if err != nil {
		t.Fatalf("BuildOutbound: %v", err)
	}
	ext := msg.GetExtendedTextMessage()
	if ext == nil || ext.GetText() != "encaminhada" {
		t.Fatalf("expected ExtendedTextMessage, got %+v", msg)
	}
	if !ext.GetContextInfo().GetIsForwarded() {
		t.Fatalf("expected IsForwarded=true")
	}
}

func TestBuildOutboundRoutesButtonsAndList(t *testing.T) {
	to, msg, nodes, err := mapper.BuildOutbound(context.Background(), stubUploader{}, amqp.GatewaySendCommand{
		To:          "5511999998888@s.whatsapp.net",
		Type:        "buttons",
		Interactive: &amqp.InteractivePayload{Body: "oi", Buttons: []amqp.InteractiveButton{{Text: "sim"}}},
	}, stubFetch(nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	if to.User != "5511999998888" {
		t.Fatalf("to = %s", to)
	}
	if msg.GetInteractiveMessage() == nil {
		t.Fatal("expected an interactive message")
	}
	if len(nodes) == 0 {
		t.Fatal("buttons must carry their binary nodes")
	}

	_, listMsg, listNodes, err := mapper.BuildOutbound(context.Background(), stubUploader{}, amqp.GatewaySendCommand{
		To:          "5511999998888@s.whatsapp.net",
		Type:        "list",
		Interactive: &amqp.InteractivePayload{Body: "oi", ButtonText: "abrir", Sections: []amqp.InteractiveSection{{Rows: []amqp.InteractiveRow{{Title: "a"}}}}},
	}, stubFetch(nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	if listMsg.GetDocumentWithCaptionMessage().GetMessage().GetListMessage() == nil {
		t.Fatal("expected a list message")
	}
	if len(listNodes) == 0 {
		t.Fatal("list must carry its binary nodes")
	}
}

func TestBuildOutboundButtonsReply(t *testing.T) {
	_, msg, _, err := mapper.BuildOutbound(context.Background(), stubUploader{}, amqp.GatewaySendCommand{
		To:          "5511999998888@s.whatsapp.net",
		Type:        "buttons",
		Interactive: &amqp.InteractivePayload{Body: "oi", Buttons: []amqp.InteractiveButton{{ID: "b1", Text: "sim"}}},
		ReplyTo: &amqp.ReplyToPayload{
			ProviderMessageID: "wamid.quoted-buttons",
			ParticipantJID:    "5511888888888@s.whatsapp.net",
		},
	}, stubFetch(nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	ctxInfo := msg.GetInteractiveMessage().GetContextInfo()
	if ctxInfo.GetStanzaID() != "wamid.quoted-buttons" {
		t.Fatalf("expected ContextInfo.StanzaID=wamid.quoted-buttons, got %q", ctxInfo.GetStanzaID())
	}
	if ctxInfo.GetParticipant() != "5511888888888@s.whatsapp.net" {
		t.Fatalf("expected Participant, got %q", ctxInfo.GetParticipant())
	}
}

func TestBuildOutboundListReply(t *testing.T) {
	_, msg, _, err := mapper.BuildOutbound(context.Background(), stubUploader{}, amqp.GatewaySendCommand{
		To:          "5511999998888@s.whatsapp.net",
		Type:        "list",
		Interactive: &amqp.InteractivePayload{Body: "oi", ButtonText: "abrir", Sections: []amqp.InteractiveSection{{Rows: []amqp.InteractiveRow{{Title: "a"}}}}},
		ReplyTo:     &amqp.ReplyToPayload{ProviderMessageID: "wamid.quoted-list"},
	}, stubFetch(nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	list := msg.GetDocumentWithCaptionMessage().GetMessage().GetListMessage()
	if list == nil {
		t.Fatalf("expected a list message, got %+v", msg)
	}
	if list.GetContextInfo().GetStanzaID() != "wamid.quoted-list" {
		t.Fatalf("expected ContextInfo.StanzaID=wamid.quoted-list, got %q", list.GetContextInfo().GetStanzaID())
	}
}

func TestBuildOutboundStillReturnsNoNodesForAPlainText(t *testing.T) {
	_, _, nodes, err := mapper.BuildOutbound(context.Background(), stubUploader{}, amqp.GatewaySendCommand{
		To: "5511999998888@s.whatsapp.net", Type: "text", Text: "oi",
	}, stubFetch(nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	if nodes != nil {
		t.Fatalf("a plain text send must carry no nodes, got %+v", nodes)
	}
}

func TestBuildOutboundRefusesAnInteractiveTypeWithNoPayload(t *testing.T) {
	if _, _, _, err := mapper.BuildOutbound(context.Background(), stubUploader{}, amqp.GatewaySendCommand{
		To: "5511999998888@s.whatsapp.net", Type: "buttons",
	}, stubFetch(nil, nil)); err == nil {
		t.Fatal("expected an error when the interactive payload is missing")
	}
}
