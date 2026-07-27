package mapper_test

import (
	"context"
	"errors"
	"testing"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"

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

func stubFetch(data []byte, err error) mapper.MediaFetcher {
	return func(ctx context.Context, url string) ([]byte, error) {
		return data, err
	}
}

func TestBuildOutboundText(t *testing.T) {
	cmd := amqp.GatewaySendCommand{To: "5511999999999", Type: "text", Text: "hello"}

	to, msg, err := mapper.BuildOutbound(context.Background(), stubUploader{}, cmd, stubFetch(nil, nil))
	if err != nil {
		t.Fatalf("BuildOutbound: %v", err)
	}
	if to != types.NewJID("5511999999999", types.DefaultUserServer) {
		t.Fatalf("unexpected JID: %v", to)
	}
	if msg.GetConversation() != "hello" {
		t.Fatalf("expected Conversation=hello, got %q", msg.GetConversation())
	}
}

func TestBuildOutboundTextStripsLeadingPlus(t *testing.T) {
	cmd := amqp.GatewaySendCommand{To: "+5511999999999", Type: "text", Text: "hello"}

	to, _, err := mapper.BuildOutbound(context.Background(), stubUploader{}, cmd, stubFetch(nil, nil))
	if err != nil {
		t.Fatalf("BuildOutbound: %v", err)
	}
	if to != types.NewJID("5511999999999", types.DefaultUserServer) {
		t.Fatalf("unexpected JID: %v", to)
	}
}

func TestBuildOutboundTextReply(t *testing.T) {
	cmd := amqp.GatewaySendCommand{
		To:   "5511999999999",
		Type: "text",
		Text: "hello again",
		ReplyTo: &amqp.ReplyToPayload{
			ProviderMessageID: "wamid.stub",
			ParticipantJID:    "5511888888888@s.whatsapp.net",
		},
	}

	_, msg, err := mapper.BuildOutbound(context.Background(), stubUploader{}, cmd, stubFetch(nil, nil))
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
		To:   "5511999999999",
		Type: "image",
		Text: "a caption",
		Media: &amqp.MediaPayload{
			URL:  "https://s3.example/media.jpg",
			Mime: "image/jpeg",
		},
	}

	_, msg, err := mapper.BuildOutbound(context.Background(), stubUploader{resp: stubUploadResponse}, cmd, stubFetch([]byte("bytes"), nil))
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

func TestBuildOutboundVideo(t *testing.T) {
	cmd := amqp.GatewaySendCommand{
		To:   "5511999999999",
		Type: "video",
		Text: "a video caption",
		Media: &amqp.MediaPayload{
			URL:  "https://s3.example/media.mp4",
			Mime: "video/mp4",
		},
	}

	_, msg, err := mapper.BuildOutbound(context.Background(), stubUploader{resp: stubUploadResponse}, cmd, stubFetch([]byte("bytes"), nil))
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
		To:   "5511999999999",
		Type: "audio",
		Media: &amqp.MediaPayload{
			URL:  "https://s3.example/media.ogg",
			Mime: "audio/ogg",
		},
	}

	_, msg, err := mapper.BuildOutbound(context.Background(), stubUploader{resp: stubUploadResponse}, cmd, stubFetch([]byte("bytes"), nil))
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
		To:   "5511999999999",
		Type: "document",
		Text: "doc caption",
		Media: &amqp.MediaPayload{
			URL:      "https://s3.example/media.pdf",
			Mime:     "application/pdf",
			Filename: "invoice.pdf",
		},
	}

	_, msg, err := mapper.BuildOutbound(context.Background(), stubUploader{resp: stubUploadResponse}, cmd, stubFetch([]byte("bytes"), nil))
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
		To:    "5511999999999",
		Type:  "image",
		Media: &amqp.MediaPayload{URL: "https://s3.example/media.jpg", Mime: "image/jpeg"},
	}

	fetchErr := errors.New("boom")
	_, _, err := mapper.BuildOutbound(context.Background(), stubUploader{}, cmd, stubFetch(nil, fetchErr))
	if err == nil {
		t.Fatal("expected error when media fetch fails")
	}
}

func TestBuildOutboundMediaUploadError(t *testing.T) {
	cmd := amqp.GatewaySendCommand{
		To:    "5511999999999",
		Type:  "image",
		Media: &amqp.MediaPayload{URL: "https://s3.example/media.jpg", Mime: "image/jpeg"},
	}

	uploadErr := errors.New("upload failed")
	_, _, err := mapper.BuildOutbound(context.Background(), stubUploader{err: uploadErr}, cmd, stubFetch([]byte("bytes"), nil))
	if err == nil {
		t.Fatal("expected error when upload fails")
	}
}

func TestBuildOutboundMediaMissingPayload(t *testing.T) {
	cmd := amqp.GatewaySendCommand{To: "5511999999999", Type: "image"}

	_, _, err := mapper.BuildOutbound(context.Background(), stubUploader{}, cmd, stubFetch(nil, nil))
	if err == nil {
		t.Fatal("expected error when media payload is missing")
	}
}

func TestBuildOutboundLocation(t *testing.T) {
	cmd := amqp.GatewaySendCommand{
		To:   "5511999999999",
		Type: "location",
		Location: &amqp.LocationPayload{
			Lat:     -23.55052,
			Lng:     -46.633308,
			Name:    "Sender HQ",
			Address: "Av. Paulista, 1000",
		},
	}

	_, msg, err := mapper.BuildOutbound(context.Background(), stubUploader{}, cmd, stubFetch(nil, nil))
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

func TestBuildOutboundLocationMissingPayload(t *testing.T) {
	cmd := amqp.GatewaySendCommand{To: "5511999999999", Type: "location"}

	_, _, err := mapper.BuildOutbound(context.Background(), stubUploader{}, cmd, stubFetch(nil, nil))
	if err == nil {
		t.Fatal("expected error when location payload is missing")
	}
}

func TestBuildOutboundSingleContact(t *testing.T) {
	cmd := amqp.GatewaySendCommand{
		To:   "5511999999999",
		Type: "contacts",
		Contacts: []amqp.ContactPayload{
			{Name: "Jane Doe", Vcard: "BEGIN:VCARD\nVERSION:3.0\nFN:Jane Doe\nEND:VCARD"},
		},
	}

	_, msg, err := mapper.BuildOutbound(context.Background(), stubUploader{}, cmd, stubFetch(nil, nil))
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
		To:   "5511999999999",
		Type: "contacts",
		Contacts: []amqp.ContactPayload{
			{Name: "Jane Doe", Vcard: "BEGIN:VCARD\nFN:Jane Doe\nEND:VCARD"},
			{Name: "John Roe", Vcard: "BEGIN:VCARD\nFN:John Roe\nEND:VCARD"},
		},
	}

	_, msg, err := mapper.BuildOutbound(context.Background(), stubUploader{}, cmd, stubFetch(nil, nil))
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
	cmd := amqp.GatewaySendCommand{To: "5511999999999", Type: "contacts"}

	_, _, err := mapper.BuildOutbound(context.Background(), stubUploader{}, cmd, stubFetch(nil, nil))
	if err == nil {
		t.Fatal("expected error when contacts payload is missing")
	}
}

func TestBuildOutboundUnknownType(t *testing.T) {
	cmd := amqp.GatewaySendCommand{To: "5511999999999", Type: "sticker"}

	_, _, err := mapper.BuildOutbound(context.Background(), stubUploader{}, cmd, stubFetch(nil, nil))
	if err == nil {
		t.Fatal("expected error for unknown type")
	}
}
