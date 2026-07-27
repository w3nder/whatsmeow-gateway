package mapper_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/w3nder/whatsmeow-gateway/internal/mapper"
)

type recordedPut struct {
	key  string
	mime string
	data []byte
}

type fakeDownloader struct {
	data []byte
	err  error
}

func (f fakeDownloader) Download(ctx context.Context, msg whatsmeow.DownloadableMessage) ([]byte, error) {
	return f.data, f.err
}

type fakeMediaStore struct {
	puts []recordedPut
	err  error
}

func (f *fakeMediaStore) Put(ctx context.Context, key, mime string, data []byte) error {
	if f.err != nil {
		return f.err
	}
	f.puts = append(f.puts, recordedPut{key: key, mime: mime, data: data})
	return nil
}

func baseInfo(id, from string) types.MessageInfo {
	return types.MessageInfo{
		MessageSource: types.MessageSource{Sender: types.NewJID(from, types.DefaultUserServer)},
		ID:            id,
		PushName:      "Jane Doe",
		Timestamp:     time.Unix(1700000000, 0),
	}
}

func TestBuildInboundText(t *testing.T) {
	evt := &events.Message{
		Info:    baseInfo("wamid.text-1", "5511999999999"),
		Message: &waE2E.Message{Conversation: proto.String("hello there")},
	}

	out, err := mapper.BuildInbound(context.Background(), fakeDownloader{}, &fakeMediaStore{}, "channel-1", "tenant-1", evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.PhoneNumberID != "channel-1" {
		t.Fatalf("expected PhoneNumberID=channel-1, got %q", out.PhoneNumberID)
	}
	if out.From != "5511999999999" {
		t.Fatalf("expected From=5511999999999, got %q", out.From)
	}
	if out.ProfileName != "Jane Doe" {
		t.Fatalf("expected ProfileName=Jane Doe, got %q", out.ProfileName)
	}
	if out.ProviderMessageID != "wamid.text-1" {
		t.Fatalf("expected ProviderMessageID=wamid.text-1, got %q", out.ProviderMessageID)
	}
	if out.Timestamp != "1700000000" {
		t.Fatalf("expected Timestamp=1700000000, got %q", out.Timestamp)
	}
	if out.Type != "text" {
		t.Fatalf("expected Type=text, got %q", out.Type)
	}
	if out.Text == nil || out.Text.Body != "hello there" {
		t.Fatalf("expected Text.Body=hello there, got %+v", out.Text)
	}
}

func TestBuildInboundExtendedTextWithReply(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.text-2", "5511999999999"),
		Message: &waE2E.Message{
			ExtendedTextMessage: &waE2E.ExtendedTextMessage{
				Text:        proto.String("a reply"),
				ContextInfo: &waE2E.ContextInfo{StanzaID: proto.String("wamid.quoted")},
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), fakeDownloader{}, &fakeMediaStore{}, "channel-1", "tenant-1", evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "text" {
		t.Fatalf("expected Type=text, got %q", out.Type)
	}
	if out.Text == nil || out.Text.Body != "a reply" {
		t.Fatalf("expected Text.Body=a reply, got %+v", out.Text)
	}
	if out.ContextMessageID != "wamid.quoted" {
		t.Fatalf("expected ContextMessageID=wamid.quoted, got %q", out.ContextMessageID)
	}
}

func TestBuildInboundImageDownloadsAndStores(t *testing.T) {
	dl := fakeDownloader{data: []byte("image bytes")}
	store := &fakeMediaStore{}
	evt := &events.Message{
		Info: baseInfo("wamid.image-1", "5511999999999"),
		Message: &waE2E.Message{
			ImageMessage: &waE2E.ImageMessage{
				Mimetype:    proto.String("image/jpeg"),
				Caption:     proto.String("a caption"),
				ContextInfo: &waE2E.ContextInfo{StanzaID: proto.String("wamid.quoted-image")},
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), dl, store, "channel-1", "tenant-1", evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "image" {
		t.Fatalf("expected Type=image, got %q", out.Type)
	}
	if out.Media == nil {
		t.Fatalf("expected Media, got nil")
	}
	if out.Media.MimeType != "image/jpeg" {
		t.Fatalf("expected MimeType=image/jpeg, got %q", out.Media.MimeType)
	}
	if out.Media.Caption != "a caption" {
		t.Fatalf("expected Caption='a caption', got %q", out.Media.Caption)
	}
	if out.Media.Key == "" {
		t.Fatalf("expected non-empty media key")
	}
	if out.ContextMessageID != "wamid.quoted-image" {
		t.Fatalf("expected ContextMessageID=wamid.quoted-image, got %q", out.ContextMessageID)
	}

	if len(store.puts) != 1 {
		t.Fatalf("expected exactly 1 store.Put call, got %d", len(store.puts))
	}
	put := store.puts[0]
	if put.key != out.Media.Key {
		t.Fatalf("expected stored key to match emitted media key, got %q vs %q", put.key, out.Media.Key)
	}
	if put.mime != "image/jpeg" {
		t.Fatalf("expected stored mime=image/jpeg, got %q", put.mime)
	}
	if string(put.data) != "image bytes" {
		t.Fatalf("expected stored data='image bytes', got %q", put.data)
	}
}

func TestBuildInboundVideo(t *testing.T) {
	dl := fakeDownloader{data: []byte("video bytes")}
	store := &fakeMediaStore{}
	evt := &events.Message{
		Info: baseInfo("wamid.video-1", "5511999999999"),
		Message: &waE2E.Message{
			VideoMessage: &waE2E.VideoMessage{
				Mimetype: proto.String("video/mp4"),
				Caption:  proto.String("a video"),
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), dl, store, "channel-1", "tenant-1", evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "video" {
		t.Fatalf("expected Type=video, got %q", out.Type)
	}
	if out.Media == nil || out.Media.MimeType != "video/mp4" || out.Media.Caption != "a video" {
		t.Fatalf("unexpected media: %+v", out.Media)
	}
}

func TestBuildInboundAudio(t *testing.T) {
	dl := fakeDownloader{data: []byte("audio bytes")}
	store := &fakeMediaStore{}
	evt := &events.Message{
		Info: baseInfo("wamid.audio-1", "5511999999999"),
		Message: &waE2E.Message{
			AudioMessage: &waE2E.AudioMessage{
				Mimetype: proto.String("audio/ogg"),
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), dl, store, "channel-1", "tenant-1", evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "audio" {
		t.Fatalf("expected Type=audio, got %q", out.Type)
	}
	if out.Media == nil || out.Media.MimeType != "audio/ogg" {
		t.Fatalf("unexpected media: %+v", out.Media)
	}
}

func TestBuildInboundDocument(t *testing.T) {
	dl := fakeDownloader{data: []byte("doc bytes")}
	store := &fakeMediaStore{}
	evt := &events.Message{
		Info: baseInfo("wamid.doc-1", "5511999999999"),
		Message: &waE2E.Message{
			DocumentMessage: &waE2E.DocumentMessage{
				Mimetype: proto.String("application/pdf"),
				FileName: proto.String("invoice.pdf"),
				Caption:  proto.String("doc caption"),
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), dl, store, "channel-1", "tenant-1", evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "document" {
		t.Fatalf("expected Type=document, got %q", out.Type)
	}
	if out.Media == nil || out.Media.MimeType != "application/pdf" || out.Media.Filename != "invoice.pdf" || out.Media.Caption != "doc caption" {
		t.Fatalf("unexpected media: %+v", out.Media)
	}
}

func TestBuildInboundMediaDownloadErrorPropagates(t *testing.T) {
	dl := fakeDownloader{err: errors.New("download failed")}
	store := &fakeMediaStore{}
	evt := &events.Message{
		Info:    baseInfo("wamid.image-err", "5511999999999"),
		Message: &waE2E.Message{ImageMessage: &waE2E.ImageMessage{Mimetype: proto.String("image/jpeg")}},
	}

	_, err := mapper.BuildInbound(context.Background(), dl, store, "channel-1", "tenant-1", evt)
	if err == nil {
		t.Fatal("expected error when download fails")
	}
	if len(store.puts) != 0 {
		t.Fatalf("expected no Put calls after download failure, got %d", len(store.puts))
	}
}

func TestBuildInboundMediaStoreErrorPropagates(t *testing.T) {
	dl := fakeDownloader{data: []byte("bytes")}
	store := &fakeMediaStore{err: errors.New("upload failed")}
	evt := &events.Message{
		Info:    baseInfo("wamid.image-err2", "5511999999999"),
		Message: &waE2E.Message{ImageMessage: &waE2E.ImageMessage{Mimetype: proto.String("image/jpeg")}},
	}

	_, err := mapper.BuildInbound(context.Background(), dl, store, "channel-1", "tenant-1", evt)
	if err == nil {
		t.Fatal("expected error when media store fails")
	}
}

func TestBuildInboundLocation(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.location-1", "5511999999999"),
		Message: &waE2E.Message{
			LocationMessage: &waE2E.LocationMessage{
				DegreesLatitude:  proto.Float64(-23.55052),
				DegreesLongitude: proto.Float64(-46.633308),
				Name:             proto.String("Sender HQ"),
				Address:          proto.String("Av. Paulista, 1000"),
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), fakeDownloader{}, &fakeMediaStore{}, "channel-1", "tenant-1", evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "location" {
		t.Fatalf("expected Type=location, got %q", out.Type)
	}
	if out.Location == nil {
		t.Fatalf("expected Location, got nil")
	}
	if out.Location.Latitude != -23.55052 || out.Location.Longitude != -46.633308 {
		t.Fatalf("unexpected lat/lng: %+v", out.Location)
	}
	if out.Location.Name != "Sender HQ" || out.Location.Address != "Av. Paulista, 1000" {
		t.Fatalf("unexpected name/address: %+v", out.Location)
	}
}

func TestBuildInboundLocationZeroCoordinatesSurviveRoundTrip(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.location-2", "5511999999999"),
		Message: &waE2E.Message{
			LocationMessage: &waE2E.LocationMessage{
				DegreesLatitude:  proto.Float64(0),
				DegreesLongitude: proto.Float64(0),
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), fakeDownloader{}, &fakeMediaStore{}, "channel-1", "tenant-1", evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Location == nil {
		t.Fatalf("expected Location, got nil")
	}

	raw, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	location, ok := decoded["location"].(map[string]any)
	if !ok {
		t.Fatalf("expected location object in JSON, got %v", decoded["location"])
	}
	if _, ok := location["latitude"]; !ok {
		t.Fatalf("expected latitude field present in JSON even when 0, got %v", location)
	}
	if _, ok := location["longitude"]; !ok {
		t.Fatalf("expected longitude field present in JSON even when 0, got %v", location)
	}
	if location["latitude"] != 0.0 || location["longitude"] != 0.0 {
		t.Fatalf("expected latitude=0 longitude=0, got %v", location)
	}
}

func TestBuildInboundContact(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.contact-1", "5511999999999"),
		Message: &waE2E.Message{
			ContactMessage: &waE2E.ContactMessage{
				DisplayName: proto.String("John Roe"),
				Vcard:       proto.String("BEGIN:VCARD\nFN:John Roe\nEND:VCARD"),
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), fakeDownloader{}, &fakeMediaStore{}, "channel-1", "tenant-1", evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "contacts" {
		t.Fatalf("expected Type=contacts, got %q", out.Type)
	}
	if len(out.Contacts) != 1 {
		t.Fatalf("expected 1 contact, got %d", len(out.Contacts))
	}
	if out.Contacts[0].Name == nil || out.Contacts[0].Name.FormattedName != "John Roe" {
		t.Fatalf("expected formatted name John Roe, got %+v", out.Contacts[0].Name)
	}
	if out.Contacts[0].Vcard == "" {
		t.Fatalf("expected non-empty vcard")
	}
}

func TestBuildInboundContactsArray(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.contacts-array-1", "5511999999999"),
		Message: &waE2E.Message{
			ContactsArrayMessage: &waE2E.ContactsArrayMessage{
				Contacts: []*waE2E.ContactMessage{
					{DisplayName: proto.String("Jane Doe"), Vcard: proto.String("BEGIN:VCARD\nFN:Jane Doe\nEND:VCARD")},
					{DisplayName: proto.String("John Roe"), Vcard: proto.String("BEGIN:VCARD\nFN:John Roe\nEND:VCARD")},
				},
				ContextInfo: &waE2E.ContextInfo{StanzaID: proto.String("wamid.quoted-contacts")},
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), fakeDownloader{}, &fakeMediaStore{}, "channel-1", "tenant-1", evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "contacts" {
		t.Fatalf("expected Type=contacts, got %q", out.Type)
	}
	if len(out.Contacts) != 2 {
		t.Fatalf("expected 2 contacts, got %d", len(out.Contacts))
	}
	if out.Contacts[0].Name == nil || out.Contacts[0].Name.FormattedName != "Jane Doe" {
		t.Fatalf("expected first contact Jane Doe, got %+v", out.Contacts[0].Name)
	}
	if out.Contacts[1].Name == nil || out.Contacts[1].Name.FormattedName != "John Roe" {
		t.Fatalf("expected second contact John Roe, got %+v", out.Contacts[1].Name)
	}
	if out.Contacts[0].Vcard == "" || out.Contacts[1].Vcard == "" {
		t.Fatalf("expected non-empty vcards, got %+v", out.Contacts)
	}
	if out.ContextMessageID != "wamid.quoted-contacts" {
		t.Fatalf("expected ContextMessageID=wamid.quoted-contacts, got %q", out.ContextMessageID)
	}
}

func TestBuildInboundReaction(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.reaction-1", "5511999999999"),
		Message: &waE2E.Message{
			ReactionMessage: &waE2E.ReactionMessage{
				Key:  &waCommon.MessageKey{ID: proto.String("wamid.target")},
				Text: proto.String("👍"),
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), fakeDownloader{}, &fakeMediaStore{}, "channel-1", "tenant-1", evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "reaction" {
		t.Fatalf("expected Type=reaction, got %q", out.Type)
	}
	if out.Reaction == nil {
		t.Fatalf("expected Reaction, got nil")
	}
	if out.Reaction.MessageID != "wamid.target" {
		t.Fatalf("expected Reaction.MessageID=wamid.target, got %q", out.Reaction.MessageID)
	}
	if out.Reaction.Emoji != "👍" {
		t.Fatalf("expected Reaction.Emoji=thumbsup, got %q", out.Reaction.Emoji)
	}
}

func TestBuildInboundUnsupportedMessage(t *testing.T) {
	evt := &events.Message{
		Info:    baseInfo("wamid.unsupported-1", "5511999999999"),
		Message: &waE2E.Message{},
	}

	_, err := mapper.BuildInbound(context.Background(), fakeDownloader{}, &fakeMediaStore{}, "channel-1", "tenant-1", evt)
	if err == nil {
		t.Fatal("expected error for unsupported message")
	}
	if !errors.Is(err, mapper.ErrSkip) {
		t.Fatalf("expected mapper.ErrSkip for an empty message, got %v", err)
	}
}

func TestBuildInboundProtocolMessageIsSkipped(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.protocol-1", "5511999999999"),
		Message: &waE2E.Message{
			ProtocolMessage: &waE2E.ProtocolMessage{
				Type: waE2E.ProtocolMessage_MESSAGE_EDIT.Enum(),
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), fakeDownloader{}, &fakeMediaStore{}, "channel-1", "tenant-1", evt)
	if !errors.Is(err, mapper.ErrSkip) {
		t.Fatalf("expected mapper.ErrSkip for a protocolMessage, got %v", err)
	}
	if out.Type != "" || out.Text != nil || out.Media != nil {
		t.Fatalf("expected zero-value InboundEvent for a skipped message, got %+v", out)
	}
}

func TestBuildInboundSenderKeyDistributionMessageIsSkipped(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.skd-1", "5511999999999"),
		Message: &waE2E.Message{
			SenderKeyDistributionMessage: &waE2E.SenderKeyDistributionMessage{
				GroupID: proto.String("120363000000000000@g.us"),
			},
		},
	}

	_, err := mapper.BuildInbound(context.Background(), fakeDownloader{}, &fakeMediaStore{}, "channel-1", "tenant-1", evt)
	if !errors.Is(err, mapper.ErrSkip) {
		t.Fatalf("expected mapper.ErrSkip for a senderKeyDistributionMessage, got %v", err)
	}
}

func TestBuildInboundEmptyMessageIsSkipped(t *testing.T) {
	evt := &events.Message{
		Info:    baseInfo("wamid.empty-1", "5511999999999"),
		Message: &waE2E.Message{},
	}

	_, err := mapper.BuildInbound(context.Background(), fakeDownloader{}, &fakeMediaStore{}, "channel-1", "tenant-1", evt)
	if !errors.Is(err, mapper.ErrSkip) {
		t.Fatalf("expected mapper.ErrSkip for an empty message, got %v", err)
	}
}

func TestBuildInboundEphemeralWrappedTextIsUnwrapped(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.ephemeral-text-1", "5511999999999"),
		Message: &waE2E.Message{
			EphemeralMessage: &waE2E.FutureProofMessage{
				Message: &waE2E.Message{Conversation: proto.String("disappearing hello")},
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), fakeDownloader{}, &fakeMediaStore{}, "channel-1", "tenant-1", evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "text" {
		t.Fatalf("expected Type=text, got %q", out.Type)
	}
	if out.Text == nil || out.Text.Body != "disappearing hello" {
		t.Fatalf("expected Text.Body=disappearing hello, got %+v", out.Text)
	}
}

func TestBuildInboundEphemeralWrappedImageIsUnwrappedAndDownloaded(t *testing.T) {
	dl := fakeDownloader{data: []byte("ephemeral image bytes")}
	store := &fakeMediaStore{}
	evt := &events.Message{
		Info: baseInfo("wamid.ephemeral-image-1", "5511999999999"),
		Message: &waE2E.Message{
			EphemeralMessage: &waE2E.FutureProofMessage{
				Message: &waE2E.Message{
					ImageMessage: &waE2E.ImageMessage{
						Mimetype: proto.String("image/jpeg"),
						Caption:  proto.String("ephemeral caption"),
					},
				},
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), dl, store, "channel-1", "tenant-1", evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "image" {
		t.Fatalf("expected Type=image, got %q", out.Type)
	}
	if out.Media == nil || out.Media.MimeType != "image/jpeg" || out.Media.Caption != "ephemeral caption" {
		t.Fatalf("unexpected media: %+v", out.Media)
	}
	if len(store.puts) != 1 {
		t.Fatalf("expected exactly 1 store.Put call, got %d", len(store.puts))
	}
}

func TestBuildInboundViewOnceWrappedMessageIsUnwrapped(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.viewonce-1", "5511999999999"),
		Message: &waE2E.Message{
			ViewOnceMessage: &waE2E.FutureProofMessage{
				Message: &waE2E.Message{
					ImageMessage: &waE2E.ImageMessage{Mimetype: proto.String("image/png")},
				},
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), fakeDownloader{data: []byte("bytes")}, &fakeMediaStore{}, "channel-1", "tenant-1", evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "image" {
		t.Fatalf("expected Type=image, got %q", out.Type)
	}
}

func TestBuildInboundViewOnceV2WrappedMessageIsUnwrapped(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.viewoncev2-1", "5511999999999"),
		Message: &waE2E.Message{
			ViewOnceMessageV2: &waE2E.FutureProofMessage{
				Message: &waE2E.Message{
					VideoMessage: &waE2E.VideoMessage{Mimetype: proto.String("video/mp4")},
				},
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), fakeDownloader{data: []byte("bytes")}, &fakeMediaStore{}, "channel-1", "tenant-1", evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "video" {
		t.Fatalf("expected Type=video, got %q", out.Type)
	}
}

func TestBuildInboundViewOnceV2ExtensionWrappedMessageIsUnwrapped(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.viewoncev2ext-1", "5511999999999"),
		Message: &waE2E.Message{
			ViewOnceMessageV2Extension: &waE2E.FutureProofMessage{
				Message: &waE2E.Message{
					AudioMessage: &waE2E.AudioMessage{Mimetype: proto.String("audio/ogg")},
				},
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), fakeDownloader{data: []byte("bytes")}, &fakeMediaStore{}, "channel-1", "tenant-1", evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "audio" {
		t.Fatalf("expected Type=audio, got %q", out.Type)
	}
}

func TestBuildInboundDocumentWithCaptionWrappedMessageIsUnwrapped(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.docwithcaption-1", "5511999999999"),
		Message: &waE2E.Message{
			DocumentWithCaptionMessage: &waE2E.FutureProofMessage{
				Message: &waE2E.Message{
					DocumentMessage: &waE2E.DocumentMessage{
						Mimetype: proto.String("application/pdf"),
						FileName: proto.String("wrapped.pdf"),
					},
				},
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), fakeDownloader{data: []byte("bytes")}, &fakeMediaStore{}, "channel-1", "tenant-1", evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "document" {
		t.Fatalf("expected Type=document, got %q", out.Type)
	}
	if out.Media == nil || out.Media.Filename != "wrapped.pdf" {
		t.Fatalf("unexpected media: %+v", out.Media)
	}
}

func TestBuildInboundNestedEphemeralWrappingDocumentWithCaptionIsUnwrapped(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.nested-1", "5511999999999"),
		Message: &waE2E.Message{
			EphemeralMessage: &waE2E.FutureProofMessage{
				Message: &waE2E.Message{
					DocumentWithCaptionMessage: &waE2E.FutureProofMessage{
						Message: &waE2E.Message{
							DocumentMessage: &waE2E.DocumentMessage{
								Mimetype: proto.String("application/pdf"),
								FileName: proto.String("nested.pdf"),
							},
						},
					},
				},
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), fakeDownloader{data: []byte("bytes")}, &fakeMediaStore{}, "channel-1", "tenant-1", evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "document" {
		t.Fatalf("expected Type=document, got %q", out.Type)
	}
	if out.Media == nil || out.Media.Filename != "nested.pdf" {
		t.Fatalf("unexpected media: %+v", out.Media)
	}
}

func TestBuildInboundUnknownAfterUnwrapIsSkipped(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.unknown-after-unwrap-1", "5511999999999"),
		Message: &waE2E.Message{
			EphemeralMessage: &waE2E.FutureProofMessage{
				Message: &waE2E.Message{
					StickerMessage: &waE2E.StickerMessage{Mimetype: proto.String("image/webp")},
				},
			},
		},
	}

	_, err := mapper.BuildInbound(context.Background(), fakeDownloader{}, &fakeMediaStore{}, "channel-1", "tenant-1", evt)
	if !errors.Is(err, mapper.ErrSkip) {
		t.Fatalf("expected mapper.ErrSkip for an unknown message after unwrap, got %v", err)
	}
}

func TestBuildStatusDelivered(t *testing.T) {
	evt := &events.Receipt{
		MessageIDs: []types.MessageID{"wamid.1"},
		Type:       types.ReceiptTypeDelivered,
		Timestamp:  time.Unix(1700000100, 0),
	}

	out := mapper.BuildStatus(evt)
	if len(out) != 1 {
		t.Fatalf("expected 1 status event, got %d", len(out))
	}
	if out[0].ProviderMessageID != "wamid.1" {
		t.Fatalf("expected ProviderMessageID=wamid.1, got %q", out[0].ProviderMessageID)
	}
	if out[0].Status != "delivered" {
		t.Fatalf("expected Status=delivered, got %q", out[0].Status)
	}
	if out[0].Timestamp != "1700000100" {
		t.Fatalf("expected Timestamp=1700000100, got %q", out[0].Timestamp)
	}
}

func TestBuildStatusRead(t *testing.T) {
	evt := &events.Receipt{
		MessageIDs: []types.MessageID{"wamid.1"},
		Type:       types.ReceiptTypeRead,
		Timestamp:  time.Unix(1700000200, 0),
	}

	out := mapper.BuildStatus(evt)
	if len(out) != 1 || out[0].Status != "read" {
		t.Fatalf("expected 1 read status event, got %+v", out)
	}
}

func TestBuildStatusMultipleMessageIDs(t *testing.T) {
	evt := &events.Receipt{
		MessageIDs: []types.MessageID{"wamid.1", "wamid.2", "wamid.3"},
		Type:       types.ReceiptTypeRead,
		Timestamp:  time.Unix(1700000300, 0),
	}

	out := mapper.BuildStatus(evt)
	if len(out) != 3 {
		t.Fatalf("expected 3 status events, got %d", len(out))
	}
	for i, id := range []string{"wamid.1", "wamid.2", "wamid.3"} {
		if out[i].ProviderMessageID != id {
			t.Fatalf("expected ProviderMessageID[%d]=%s, got %q", i, id, out[i].ProviderMessageID)
		}
	}
}

func TestBuildStatusUnmappedTypeReturnsEmpty(t *testing.T) {
	evt := &events.Receipt{
		MessageIDs: []types.MessageID{"wamid.1"},
		Type:       types.ReceiptTypeRetry,
		Timestamp:  time.Unix(1700000400, 0),
	}

	out := mapper.BuildStatus(evt)
	if len(out) != 0 {
		t.Fatalf("expected no status events for unmapped receipt type, got %+v", out)
	}
}
