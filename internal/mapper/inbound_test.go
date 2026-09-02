package mapper_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
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

type fakePNResolver struct {
	pn    types.JID
	found bool
	err   error
}

func (f fakePNResolver) PNForLID(ctx context.Context, lid types.JID) (types.JID, bool, error) {
	return f.pn, f.found, f.err
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

func testDeps(dl mapper.Downloader, resolver mapper.PNResolver, store mapper.MediaStore) mapper.InboundDeps {
	return mapper.InboundDeps{
		Downloader: dl,
		Resolver:   resolver,
		Media:      store,
		ChannelID:  "channel-1",
		TenantID:   "tenant-1",
	}
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

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
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

func TestBuildInboundLIDSenderWithPNAltPopulatesBothIdentifiers(t *testing.T) {
	evt := &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Sender:    types.NewJID("173907587899617", types.HiddenUserServer),
				SenderAlt: types.NewJID("5511988887777", types.DefaultUserServer),
			},
			ID:        "wamid.lid-1",
			PushName:  "Jane Doe",
			Timestamp: time.Unix(1700000000, 0),
		},
		Message: &waE2E.Message{Conversation: proto.String("hello there")},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.SenderLid != "173907587899617" {
		t.Fatalf("expected SenderLid=173907587899617, got %q", out.SenderLid)
	}
	if out.SenderPn != "5511988887777" {
		t.Fatalf("expected SenderPn=5511988887777, got %q", out.SenderPn)
	}
	if out.From != "5511988887777" {
		t.Fatalf("expected From=5511988887777, got %q", out.From)
	}
}

func TestBuildInboundLIDSenderNoAltResolvesSenderPNViaStore(t *testing.T) {
	evt := &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Sender: types.NewJID("173907587899617", types.HiddenUserServer),
			},
			ID:        "wamid.lid-2",
			PushName:  "Jane Doe",
			Timestamp: time.Unix(1700000000, 0),
		},
		Message: &waE2E.Message{Conversation: proto.String("hello there")},
	}

	resolver := fakePNResolver{pn: types.NewJID("5511999999999", types.DefaultUserServer), found: true}

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, resolver, &fakeMediaStore{}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.SenderLid != "173907587899617" {
		t.Fatalf("expected SenderLid=173907587899617, got %q", out.SenderLid)
	}
	if out.SenderPn != "5511999999999" {
		t.Fatalf("expected SenderPn=5511999999999 (resolved via store), got %q", out.SenderPn)
	}
	if out.From != "5511999999999" {
		t.Fatalf("expected From=5511999999999, got %q", out.From)
	}
}

func TestBuildInboundLIDSenderUnknownMappingKeepsLIDOnly(t *testing.T) {
	evt := &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Sender: types.NewJID("173907587899617", types.HiddenUserServer),
			},
			ID:        "wamid.lid-3",
			PushName:  "Jane Doe",
			Timestamp: time.Unix(1700000000, 0),
		},
		Message: &waE2E.Message{Conversation: proto.String("hello there")},
	}

	resolver := fakePNResolver{found: false}

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, resolver, &fakeMediaStore{}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.SenderLid != "173907587899617" {
		t.Fatalf("expected SenderLid=173907587899617, got %q", out.SenderLid)
	}
	if out.SenderPn != "" {
		t.Fatalf("expected SenderPn empty (mapping unknown), got %q", out.SenderPn)
	}
	if out.From != "173907587899617" {
		t.Fatalf("expected From=173907587899617 (last-resort LID fallback), got %q", out.From)
	}
}

func TestBuildInboundPNSenderWithLIDAltPopulatesBothIdentifiers(t *testing.T) {
	evt := &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Sender:    types.NewJID("5511999999999", types.DefaultUserServer),
				SenderAlt: types.NewJID("173907587899617", types.HiddenUserServer),
			},
			ID:        "wamid.pn-1",
			PushName:  "Jane Doe",
			Timestamp: time.Unix(1700000000, 0),
		},
		Message: &waE2E.Message{Conversation: proto.String("hello there")},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.SenderPn != "5511999999999" {
		t.Fatalf("expected SenderPn=5511999999999, got %q", out.SenderPn)
	}
	if out.SenderLid != "173907587899617" {
		t.Fatalf("expected SenderLid=173907587899617, got %q", out.SenderLid)
	}
	if out.From != "5511999999999" {
		t.Fatalf("expected From=5511999999999, got %q", out.From)
	}
}

func TestBuildInboundFromMeKeysToChatContact(t *testing.T) {
	evt := &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Sender:   types.NewJID("5511900000000", types.DefaultUserServer),
				Chat:     types.NewJID("5511999999999", types.DefaultUserServer),
				IsFromMe: true,
			},
			ID:        "wamid.fromme-1",
			PushName:  "Jane Doe",
			Timestamp: time.Unix(1700000000, 0),
		},
		Message: &waE2E.Message{Conversation: proto.String("sent from my phone")},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.ProfileName != "" {
		t.Errorf("ProfileName = %q on a fromMe echo; PushName there is the tenant's own device name, not the contact's", out.ProfileName)
	}
	if !out.FromMe {
		t.Fatalf("expected FromMe=true, got %v", out.FromMe)
	}
	if out.From != "5511999999999" {
		t.Fatalf("expected From=5511999999999 (chat contact), got %q", out.From)
	}
	if out.SenderPn != "5511999999999" {
		t.Fatalf("expected SenderPn=5511999999999 (chat contact), got %q", out.SenderPn)
	}
	if out.Type != "text" {
		t.Fatalf("expected Type=text, got %q", out.Type)
	}
	if out.Text == nil || out.Text.Body != "sent from my phone" {
		t.Fatalf("expected Text.Body='sent from my phone', got %+v", out.Text)
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

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
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

	out, err := mapper.BuildInbound(context.Background(), testDeps(dl, nil, store), evt)
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

	out, err := mapper.BuildInbound(context.Background(), testDeps(dl, nil, store), evt)
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

	out, err := mapper.BuildInbound(context.Background(), testDeps(dl, nil, store), evt)
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

	out, err := mapper.BuildInbound(context.Background(), testDeps(dl, nil, store), evt)
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

	_, err := mapper.BuildInbound(context.Background(), testDeps(dl, nil, store), evt)
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

	_, err := mapper.BuildInbound(context.Background(), testDeps(dl, nil, store), evt)
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

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
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

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
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

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
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

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
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

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
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
		Info: baseInfo("wamid.unsupported-1", "5511999999999"),
		Message: &waE2E.Message{
			ProductMessage: &waE2E.ProductMessage{},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "unsupported" {
		t.Fatalf("expected Type=unsupported, got %q", out.Type)
	}
	if out.Unsupported == nil || out.Unsupported.Type == "" {
		t.Fatalf("expected non-empty Unsupported.Type, got %+v", out.Unsupported)
	}
	if out.Unsupported.Type != "productMessage" {
		t.Fatalf("expected Unsupported.Type=productMessage, got %q", out.Unsupported.Type)
	}
}

func TestBuildInboundNonLifecycleProtocolMessageIsSkipped(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.protocol-1", "5511999999999"),
		Message: &waE2E.Message{
			ProtocolMessage: &waE2E.ProtocolMessage{
				Type: waE2E.ProtocolMessage_EPHEMERAL_SETTING.Enum(),
			},
		},
	}

	_, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if !errors.Is(err, mapper.ErrSkip) {
		t.Fatalf("expected mapper.ErrSkip for a non-lifecycle protocol message, got %v", err)
	}
}

func TestBuildInboundProtocolMessageRevoke(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.revoke-1", "5511999999999"),
		Message: &waE2E.Message{
			ProtocolMessage: &waE2E.ProtocolMessage{
				Type: waE2E.ProtocolMessage_REVOKE.Enum(),
				Key:  &waCommon.MessageKey{ID: proto.String("TARGET123")},
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "revoke" {
		t.Fatalf("expected Type=revoke, got %q", out.Type)
	}
	if out.Target == nil || out.Target.ProviderMessageID != "TARGET123" {
		t.Fatalf("expected Target.ProviderMessageID=TARGET123, got %+v", out.Target)
	}
	if out.Media != nil {
		t.Fatalf("expected no media on revoke, got %+v", out.Media)
	}
	if out.Text != nil {
		t.Fatalf("expected no text on revoke, got %+v", out.Text)
	}
}

func TestBuildInboundProtocolMessageEdit(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.edit-1", "5511999999999"),
		Message: &waE2E.Message{
			ProtocolMessage: &waE2E.ProtocolMessage{
				Type:          waE2E.ProtocolMessage_MESSAGE_EDIT.Enum(),
				Key:           &waCommon.MessageKey{ID: proto.String("TARGET123")},
				EditedMessage: &waE2E.Message{Conversation: proto.String("novo texto")},
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "edit" {
		t.Fatalf("expected Type=edit, got %q", out.Type)
	}
	if out.Target == nil || out.Target.ProviderMessageID != "TARGET123" {
		t.Fatalf("expected Target.ProviderMessageID=TARGET123, got %+v", out.Target)
	}
	if out.Text == nil || out.Text.Body != "novo texto" {
		t.Fatalf("expected Text.Body=novo texto, got %+v", out.Text)
	}
}

func TestBuildInboundProtocolMessageEditWithoutTargetIsSkipped(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.edit-empty", "5511999999999"),
		Message: &waE2E.Message{
			ProtocolMessage: &waE2E.ProtocolMessage{Type: waE2E.ProtocolMessage_MESSAGE_EDIT.Enum()},
		},
	}

	_, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if !errors.Is(err, mapper.ErrSkip) {
		t.Fatalf("expected mapper.ErrSkip for an edit protocol message with no target key, got %v", err)
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

	_, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if !errors.Is(err, mapper.ErrSkip) {
		t.Fatalf("expected mapper.ErrSkip for a senderKeyDistributionMessage, got %v", err)
	}
}

func TestBuildInboundEmptyMessageIsSkipped(t *testing.T) {
	evt := &events.Message{
		Info:    baseInfo("wamid.empty-1", "5511999999999"),
		Message: &waE2E.Message{},
	}

	_, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if !errors.Is(err, mapper.ErrSkip) {
		t.Fatalf("expected mapper.ErrSkip for an empty message, got %v", err)
	}
}

func TestBuildInboundSkipsBroadcast(t *testing.T) {
	evt := &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Chat:   types.NewJID("status", types.BroadcastServer),
				Sender: types.NewJID("5511999998888", types.DefaultUserServer),
			},
			ID: "BROADCAST1",
		},
		Message: &waE2E.Message{Conversation: proto.String("oi")},
	}
	if _, err := mapper.BuildInbound(context.Background(), mapper.InboundDeps{}, evt); !errors.Is(err, mapper.ErrSkip) {
		t.Fatalf("expected ErrSkip, got %v", err)
	}
}

type staticGroupNamer string

func (s staticGroupNamer) Name(_ context.Context, _ types.JID) string {
	return string(s)
}

func privateMessageEvent() *events.Message {
	return &events.Message{
		Info:    baseInfo("wamid.private-1", "5511999999999"),
		Message: &waE2E.Message{Conversation: proto.String("hello there")},
	}
}

func groupMessageEvent(fromMe bool) *events.Message {
	return &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Chat:     types.NewJID("120363000000000000", types.GroupServer),
				Sender:   types.NewJID("5511888887777", types.DefaultUserServer),
				IsFromMe: fromMe,
				IsGroup:  true,
			},
			ID:        "GROUPMSG1",
			PushName:  "João Silva",
			Timestamp: time.Unix(1700000000, 0),
		},
		Message: &waE2E.Message{Conversation: proto.String("bom dia pessoal")},
	}
}

func TestBuildInboundNamesTheGroupAsTheContact(t *testing.T) {
	out, err := mapper.BuildInbound(context.Background(), mapper.InboundDeps{Groups: staticGroupNamer("Vendas Nordeste")}, groupMessageEvent(false))
	if err != nil {
		t.Fatal(err)
	}
	if out.Group == nil {
		t.Fatal("expected a group block")
	}
	if out.Group.JID != "120363000000000000@g.us" {
		t.Fatalf("group jid = %q", out.Group.JID)
	}
	if out.Group.Name != "Vendas Nordeste" {
		t.Fatalf("group name = %q", out.Group.Name)
	}
	if out.From != "120363000000000000@g.us" {
		t.Fatalf("from = %q, want the group", out.From)
	}
	if out.SenderLid != "" {
		t.Fatalf("SenderLid = %q, want empty on a group event", out.SenderLid)
	}
	if out.SenderPn != "" {
		t.Fatalf("SenderPn = %q, want empty on a group event", out.SenderPn)
	}
	if out.ProfileName != "" {
		t.Fatalf("ProfileName = %q, want empty on a group event", out.ProfileName)
	}
	if out.Group.Participant == nil {
		t.Fatal("expected a participant")
	}
	if out.Group.Participant.JID != "5511888887777@s.whatsapp.net" {
		t.Fatalf("participant jid = %q", out.Group.Participant.JID)
	}
	if out.Group.Participant.Name != "João Silva" {
		t.Fatalf("participant name = %q", out.Group.Participant.Name)
	}
}

func TestBuildInboundLeavesTheParticipantEmptyOnOurOwnGroupMessage(t *testing.T) {
	out, err := mapper.BuildInbound(context.Background(), mapper.InboundDeps{Groups: staticGroupNamer("Vendas Nordeste")}, groupMessageEvent(true))
	if err != nil {
		t.Fatal(err)
	}
	if out.Group == nil {
		t.Fatal("expected a group block")
	}
	if out.Group.Participant != nil {
		t.Fatalf("participant = %+v, want nil on a message we sent", out.Group.Participant)
	}
}

func TestBuildInboundLeavesAPrivateMessageUntouched(t *testing.T) {
	out, err := mapper.BuildInbound(context.Background(), mapper.InboundDeps{}, privateMessageEvent())
	if err != nil {
		t.Fatal(err)
	}
	if out.Group != nil {
		t.Fatal("a private message must carry no group block")
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

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
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

	out, err := mapper.BuildInbound(context.Background(), testDeps(dl, nil, store), evt)
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

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{data: []byte("bytes")}, nil, &fakeMediaStore{}), evt)
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

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{data: []byte("bytes")}, nil, &fakeMediaStore{}), evt)
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

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{data: []byte("bytes")}, nil, &fakeMediaStore{}), evt)
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

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{data: []byte("bytes")}, nil, &fakeMediaStore{}), evt)
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

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{data: []byte("bytes")}, nil, &fakeMediaStore{}), evt)
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

func TestBuildInboundUnknownAfterUnwrapBecomesUnsupported(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.unknown-after-unwrap-1", "5511999999999"),
		Message: &waE2E.Message{
			EphemeralMessage: &waE2E.FutureProofMessage{
				Message: &waE2E.Message{
					ProductMessage: &waE2E.ProductMessage{},
				},
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "unsupported" {
		t.Fatalf("expected Type=unsupported, got %q", out.Type)
	}
	if out.Unsupported == nil || out.Unsupported.Type != "productMessage" {
		t.Fatalf("expected Unsupported.Type=productMessage, got %+v", out.Unsupported)
	}
}

func TestBuildInboundSticker(t *testing.T) {
	dl := fakeDownloader{data: []byte("sticker bytes")}
	store := &fakeMediaStore{}
	evt := &events.Message{
		Info: baseInfo("wamid.sticker-1", "5511999999999"),
		Message: &waE2E.Message{
			StickerMessage: &waE2E.StickerMessage{
				Mimetype:    proto.String("image/webp"),
				ContextInfo: &waE2E.ContextInfo{StanzaID: proto.String("wamid.quoted-sticker")},
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(dl, nil, store), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "sticker" {
		t.Fatalf("expected Type=sticker, got %q", out.Type)
	}
	if out.Media == nil || out.Media.MimeType != "image/webp" {
		t.Fatalf("unexpected media: %+v", out.Media)
	}
	if out.ContextMessageID != "wamid.quoted-sticker" {
		t.Fatalf("expected ContextMessageID=wamid.quoted-sticker, got %q", out.ContextMessageID)
	}
	if len(store.puts) != 1 || string(store.puts[0].data) != "sticker bytes" {
		t.Fatalf("expected sticker bytes stored, got %+v", store.puts)
	}
}

func TestBuildInboundPtvMappedToVideo(t *testing.T) {
	dl := fakeDownloader{data: []byte("ptv bytes")}
	store := &fakeMediaStore{}
	evt := &events.Message{
		Info: baseInfo("wamid.ptv-1", "5511999999999"),
		Message: &waE2E.Message{
			PtvMessage: &waE2E.VideoMessage{
				Mimetype: proto.String("video/mp4"),
				Caption:  proto.String("a ptv"),
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(dl, nil, store), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "video" {
		t.Fatalf("expected Type=video, got %q", out.Type)
	}
	if out.Media == nil || out.Media.MimeType != "video/mp4" || out.Media.Caption != "a ptv" {
		t.Fatalf("unexpected media: %+v", out.Media)
	}
}

func TestBuildInboundAudioPTTMappedToVoice(t *testing.T) {
	dl := fakeDownloader{data: []byte("voice bytes")}
	store := &fakeMediaStore{}
	evt := &events.Message{
		Info: baseInfo("wamid.voice-1", "5511999999999"),
		Message: &waE2E.Message{
			AudioMessage: &waE2E.AudioMessage{
				Mimetype: proto.String("audio/ogg"),
				PTT:      proto.Bool(true),
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(dl, nil, store), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "voice" {
		t.Fatalf("expected Type=voice, got %q", out.Type)
	}
	if out.Media == nil || out.Media.MimeType != "audio/ogg" {
		t.Fatalf("unexpected media: %+v", out.Media)
	}
}

func TestBuildInboundAudioNonPTTMappedToAudio(t *testing.T) {
	dl := fakeDownloader{data: []byte("audio bytes")}
	store := &fakeMediaStore{}
	evt := &events.Message{
		Info: baseInfo("wamid.audio-2", "5511999999999"),
		Message: &waE2E.Message{
			AudioMessage: &waE2E.AudioMessage{
				Mimetype: proto.String("audio/ogg"),
				PTT:      proto.Bool(false),
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(dl, nil, store), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "audio" {
		t.Fatalf("expected Type=audio, got %q", out.Type)
	}
}

func TestBuildInboundLiveLocation(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.live-location-1", "5511999999999"),
		Message: &waE2E.Message{
			LiveLocationMessage: &waE2E.LiveLocationMessage{
				DegreesLatitude:  proto.Float64(-23.55052),
				DegreesLongitude: proto.Float64(-46.633308),
				Caption:          proto.String("on the move"),
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "location" {
		t.Fatalf("expected Type=location, got %q", out.Type)
	}
	if out.Location == nil || !out.Location.Live {
		t.Fatalf("expected Location.Live=true, got %+v", out.Location)
	}
	if out.Location.Latitude != -23.55052 || out.Location.Longitude != -46.633308 {
		t.Fatalf("unexpected lat/lng: %+v", out.Location)
	}
}

func TestBuildInboundButtonsResponse(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.buttons-1", "5511999999999"),
		Message: &waE2E.Message{
			ButtonsResponseMessage: &waE2E.ButtonsResponseMessage{
				SelectedButtonID: proto.String("btn-1"),
				Response: &waE2E.ButtonsResponseMessage_SelectedDisplayText{
					SelectedDisplayText: "Yes please",
				},
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "text" {
		t.Fatalf("expected Type=text, got %q", out.Type)
	}
	if out.Text == nil || out.Text.Body != "Yes please" {
		t.Fatalf("expected Text.Body='Yes please', got %+v", out.Text)
	}
}

func TestBuildInboundButtonsResponseFallsBackToSelectedButtonID(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.buttons-2", "5511999999999"),
		Message: &waE2E.Message{
			ButtonsResponseMessage: &waE2E.ButtonsResponseMessage{
				SelectedButtonID: proto.String("btn-1"),
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Text == nil || out.Text.Body != "btn-1" {
		t.Fatalf("expected Text.Body='btn-1', got %+v", out.Text)
	}
}

func TestButtonReplyKeepsTheSelectedIDAlongsideTheText(t *testing.T) {
	evt := &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Chat:   types.NewJID("5511999998888", types.DefaultUserServer),
				Sender: types.NewJID("5511999998888", types.DefaultUserServer),
			},
			ID: "REPLY1",
		},
		Message: &waE2E.Message{ButtonsResponseMessage: &waE2E.ButtonsResponseMessage{
			SelectedButtonID: proto.String("opt-2"),
			Response: &waE2E.ButtonsResponseMessage_SelectedDisplayText{
				SelectedDisplayText: "Quero saber mais",
			},
		}},
	}

	out, err := mapper.BuildInbound(context.Background(), mapper.InboundDeps{}, evt)
	if err != nil {
		t.Fatal(err)
	}
	if out.Text.Body != "Quero saber mais" {
		t.Fatalf("text = %q, want today's behaviour unchanged", out.Text.Body)
	}
	if out.InteractiveReplyID != "opt-2" {
		t.Fatalf("interactiveReplyId = %q", out.InteractiveReplyID)
	}
}

func TestListReplyKeepsTheSelectedRowID(t *testing.T) {
	evt := &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Chat:   types.NewJID("5511999998888", types.DefaultUserServer),
				Sender: types.NewJID("5511999998888", types.DefaultUserServer),
			},
			ID: "REPLY2",
		},
		Message: &waE2E.Message{ListResponseMessage: &waE2E.ListResponseMessage{
			Title:             proto.String("09:00"),
			SingleSelectReply: &waE2E.ListResponseMessage_SingleSelectReply{SelectedRowID: proto.String("m1")},
		}},
	}

	out, err := mapper.BuildInbound(context.Background(), mapper.InboundDeps{}, evt)
	if err != nil {
		t.Fatal(err)
	}
	if out.Text.Body != "09:00" {
		t.Fatalf("text = %q", out.Text.Body)
	}
	if out.InteractiveReplyID != "m1" {
		t.Fatalf("interactiveReplyId = %q", out.InteractiveReplyID)
	}
}

func TestBuildInboundListResponse(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.list-1", "5511999999999"),
		Message: &waE2E.Message{
			ListResponseMessage: &waE2E.ListResponseMessage{
				Title: proto.String("Option A"),
				SingleSelectReply: &waE2E.ListResponseMessage_SingleSelectReply{
					SelectedRowID: proto.String("row-a"),
				},
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "text" {
		t.Fatalf("expected Type=text, got %q", out.Type)
	}
	if out.Text == nil || out.Text.Body != "Option A" {
		t.Fatalf("expected Text.Body='Option A', got %+v", out.Text)
	}
}

func TestBuildInboundListResponseFallsBackToSelectedRowID(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.list-2", "5511999999999"),
		Message: &waE2E.Message{
			ListResponseMessage: &waE2E.ListResponseMessage{
				SingleSelectReply: &waE2E.ListResponseMessage_SingleSelectReply{
					SelectedRowID: proto.String("row-a"),
				},
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Text == nil || out.Text.Body != "row-a" {
		t.Fatalf("expected Text.Body='row-a', got %+v", out.Text)
	}
}

func TestBuildInboundTemplateButtonReply(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.template-1", "5511999999999"),
		Message: &waE2E.Message{
			TemplateButtonReplyMessage: &waE2E.TemplateButtonReplyMessage{
				SelectedID:          proto.String("tmpl-1"),
				SelectedDisplayText: proto.String("Confirm"),
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "text" {
		t.Fatalf("expected Type=text, got %q", out.Type)
	}
	if out.Text == nil || out.Text.Body != "Confirm" {
		t.Fatalf("expected Text.Body='Confirm', got %+v", out.Text)
	}
	if out.InteractiveReplyID != "tmpl-1" {
		t.Fatalf("expected InteractiveReplyID='tmpl-1', got %q", out.InteractiveReplyID)
	}
}

func TestBuildInboundInteractiveResponse(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.interactive-1", "5511999999999"),
		Message: &waE2E.Message{
			InteractiveResponseMessage: &waE2E.InteractiveResponseMessage{
				InteractiveResponseMessage: &waE2E.InteractiveResponseMessage_NativeFlowResponseMessage_{
					NativeFlowResponseMessage: &waE2E.InteractiveResponseMessage_NativeFlowResponseMessage{
						Name:       proto.String("quick_reply"),
						ParamsJSON: proto.String(`{"id":"opt-1"}`),
					},
				},
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "text" {
		t.Fatalf("expected Type=text, got %q", out.Type)
	}
	if out.Text == nil || out.Text.Body != `{"id":"opt-1"}` {
		t.Fatalf("expected Text.Body='{\"id\":\"opt-1\"}', got %+v", out.Text)
	}
	if out.InteractiveReplyID != "opt-1" {
		t.Fatalf("expected InteractiveReplyID='opt-1', got %q", out.InteractiveReplyID)
	}
}

func TestBuildInboundInteractiveResponseWithoutAnIDLeavesTheReplyIDEmpty(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.interactive-2", "5511999999999"),
		Message: &waE2E.Message{
			InteractiveResponseMessage: &waE2E.InteractiveResponseMessage{
				InteractiveResponseMessage: &waE2E.InteractiveResponseMessage_NativeFlowResponseMessage_{
					NativeFlowResponseMessage: &waE2E.InteractiveResponseMessage_NativeFlowResponseMessage{
						Name:       proto.String("flow"),
						ParamsJSON: proto.String(`{"screen":"SURVEY"}`),
					},
				},
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "text" {
		t.Fatalf("expected Type=text, got %q", out.Type)
	}
	if out.Text == nil || out.Text.Body != `{"screen":"SURVEY"}` {
		t.Fatalf("expected the params json as the body, got %+v", out.Text)
	}
	if out.InteractiveReplyID != "" {
		t.Fatalf("expected an empty InteractiveReplyID, got %q", out.InteractiveReplyID)
	}
}

func TestBuildInboundPollCreation(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.poll-1", "5511999999999"),
		Message: &waE2E.Message{
			PollCreationMessage: &waE2E.PollCreationMessage{
				Name: proto.String("Best pizza topping?"),
				Options: []*waE2E.PollCreationMessage_Option{
					{OptionName: proto.String("Cheese")},
					{OptionName: proto.String("Pepperoni")},
				},
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "poll" {
		t.Fatalf("expected Type=poll, got %q", out.Type)
	}
	if out.RichContent == nil || out.RichContent.Poll == nil {
		t.Fatalf("expected RichContent.Poll, got %+v", out.RichContent)
	}
	p := out.RichContent.Poll
	if p.Question != "Best pizza topping?" || len(p.Options) != 2 || p.Options[0].Name != "Cheese" || p.Options[1].Name != "Pepperoni" {
		t.Fatalf("unexpected poll: %+v", p)
	}
}

func TestBuildInboundPollCreationV2(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.poll-2", "5511999999999"),
		Message: &waE2E.Message{
			PollCreationMessageV2: &waE2E.PollCreationMessage{
				Name:    proto.String("Meeting time?"),
				Options: []*waE2E.PollCreationMessage_Option{{OptionName: proto.String("10am")}},
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "poll" {
		t.Fatalf("expected Type=poll, got %q", out.Type)
	}
	if out.RichContent == nil || out.RichContent.Poll == nil || out.RichContent.Poll.Question != "Meeting time?" || len(out.RichContent.Poll.Options) != 1 {
		t.Fatalf("unexpected poll v2: %+v", out.RichContent)
	}
}

func TestBuildInboundButtonsMessage(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.buttons-1", "5511999999999"),
		Message: &waE2E.Message{
			ButtonsMessage: &waE2E.ButtonsMessage{
				ContentText: proto.String("Pick one"),
				FooterText:  proto.String("powered by vectax"),
				ContextInfo: &waE2E.ContextInfo{StanzaID: proto.String("wamid.quoted-buttons")},
				Buttons: []*waE2E.ButtonsMessage_Button{
					{
						ButtonID:   proto.String("btn-1"),
						ButtonText: &waE2E.ButtonsMessage_Button_ButtonText{DisplayText: proto.String("Yes")},
					},
					{
						ButtonID:   proto.String("btn-2"),
						ButtonText: &waE2E.ButtonsMessage_Button_ButtonText{DisplayText: proto.String("No")},
					},
				},
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "buttons" {
		t.Fatalf("expected Type=buttons, got %q", out.Type)
	}
	if out.ContextMessageID != "wamid.quoted-buttons" {
		t.Fatalf("expected ContextMessageID=wamid.quoted-buttons, got %q", out.ContextMessageID)
	}
	if out.RichContent == nil || out.RichContent.Kind != "buttons" {
		t.Fatalf("expected RichContent.Kind=buttons, got %+v", out.RichContent)
	}
	if out.RichContent.Body != "Pick one" {
		t.Fatalf("expected RichContent.Body='Pick one', got %q", out.RichContent.Body)
	}
	if out.RichContent.Footer != "powered by vectax" {
		t.Fatalf("expected RichContent.Footer='powered by vectax', got %q", out.RichContent.Footer)
	}
	if len(out.RichContent.Buttons) != 2 {
		t.Fatalf("expected 2 buttons, got %+v", out.RichContent.Buttons)
	}
	if out.RichContent.Buttons[0].ID != "btn-1" || out.RichContent.Buttons[0].Text != "Yes" {
		t.Fatalf("unexpected first button: %+v", out.RichContent.Buttons[0])
	}
	if out.RichContent.Buttons[1].ID != "btn-2" || out.RichContent.Buttons[1].Text != "No" {
		t.Fatalf("unexpected second button: %+v", out.RichContent.Buttons[1])
	}
}

func TestBuildInboundButtonsMessageEmptyFallsBackToUnsupported(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.buttons-empty-1", "5511999999999"),
		Message: &waE2E.Message{
			ButtonsMessage: &waE2E.ButtonsMessage{},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "unsupported" {
		t.Fatalf("expected Type=unsupported, got %q", out.Type)
	}
	if out.RichContent != nil {
		t.Fatalf("expected no RichContent for an empty ButtonsMessage, got %+v", out.RichContent)
	}
	if out.Unsupported == nil || out.Unsupported.Type != "buttonsMessage" {
		t.Fatalf("expected Unsupported.Type=buttonsMessage, got %+v", out.Unsupported)
	}
}

func TestBuildInboundListMessage(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.list-1", "5511999999999"),
		Message: &waE2E.Message{
			ListMessage: &waE2E.ListMessage{
				Description: proto.String("Choose your plan"),
				FooterText:  proto.String("valid today"),
				ButtonText:  proto.String("View plans"),
				ContextInfo: &waE2E.ContextInfo{StanzaID: proto.String("wamid.quoted-list")},
				Sections: []*waE2E.ListMessage_Section{
					{
						Title: proto.String("Plans"),
						Rows: []*waE2E.ListMessage_Row{
							{RowID: proto.String("row-1"), Title: proto.String("Basic"), Description: proto.String("R$ 10/mo")},
							{RowID: proto.String("row-2"), Title: proto.String("Pro"), Description: proto.String("R$ 20/mo")},
						},
					},
				},
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "list" {
		t.Fatalf("expected Type=list, got %q", out.Type)
	}
	if out.ContextMessageID != "wamid.quoted-list" {
		t.Fatalf("expected ContextMessageID=wamid.quoted-list, got %q", out.ContextMessageID)
	}
	if out.RichContent == nil || out.RichContent.Kind != "list" {
		t.Fatalf("expected RichContent.Kind=list, got %+v", out.RichContent)
	}
	if out.RichContent.Body != "Choose your plan" || out.RichContent.Footer != "valid today" {
		t.Fatalf("unexpected body/footer: %+v", out.RichContent)
	}
	if out.RichContent.List == nil || out.RichContent.List.ButtonText != "View plans" {
		t.Fatalf("expected List.ButtonText='View plans', got %+v", out.RichContent.List)
	}
	if len(out.RichContent.List.Sections) != 1 || out.RichContent.List.Sections[0].Title != "Plans" {
		t.Fatalf("unexpected sections: %+v", out.RichContent.List.Sections)
	}
	rows := out.RichContent.List.Sections[0].Rows
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %+v", rows)
	}
	if rows[0].ID != "row-1" || rows[0].Title != "Basic" || rows[0].Description != "R$ 10/mo" {
		t.Fatalf("unexpected first row: %+v", rows[0])
	}
	if rows[1].ID != "row-2" || rows[1].Title != "Pro" || rows[1].Description != "R$ 20/mo" {
		t.Fatalf("unexpected second row: %+v", rows[1])
	}
}

func TestBuildInboundListMessageEmptyFallsBackToUnsupported(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.list-empty-1", "5511999999999"),
		Message: &waE2E.Message{
			ListMessage: &waE2E.ListMessage{},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "unsupported" {
		t.Fatalf("expected Type=unsupported, got %q", out.Type)
	}
	if out.RichContent != nil {
		t.Fatalf("expected no RichContent for an empty ListMessage, got %+v", out.RichContent)
	}
	if out.Unsupported == nil || out.Unsupported.Type != "listMessage" {
		t.Fatalf("expected Unsupported.Type=listMessage, got %+v", out.Unsupported)
	}
}

func TestBuildInboundInteractiveMessageNativeFlow(t *testing.T) {
	tests := []struct {
		name       string
		buttonName string
		paramsJSON string
		wantFlow   string
		check      func(t *testing.T, rich *mapper.InboundRichContent)
	}{
		{
			name:       "quick_reply maps to buttons flow",
			buttonName: "quick_reply",
			paramsJSON: `{"display_text":"Confirmar","id":"opt-1"}`,
			wantFlow:   "buttons",
			check: func(t *testing.T, rich *mapper.InboundRichContent) {
				if len(rich.Buttons) != 1 {
					t.Fatalf("expected 1 button, got %+v", rich.Buttons)
				}
				b := rich.Buttons[0]
				if b.Text != "Confirmar" || b.ID != "opt-1" || b.Name != "quick_reply" {
					t.Fatalf("unexpected button: %+v", b)
				}
			},
		},
		{
			name:       "single_select maps to list flow",
			buttonName: "single_select",
			paramsJSON: `{"title":"Escolha uma opção","sections":[{"title":"Seção 1","rows":[{"id":"r1","title":"Linha 1","description":"desc"}]}]}`,
			wantFlow:   "list",
			check: func(t *testing.T, rich *mapper.InboundRichContent) {
				if len(rich.Buttons) != 0 {
					t.Fatalf("expected no plain buttons for single_select, got %+v", rich.Buttons)
				}
				if rich.List == nil || rich.List.ButtonText != "Escolha uma opção" {
					t.Fatalf("expected List.ButtonText='Escolha uma opção', got %+v", rich.List)
				}
				if len(rich.List.Sections) != 1 || rich.List.Sections[0].Title != "Seção 1" {
					t.Fatalf("unexpected sections: %+v", rich.List.Sections)
				}
				rows := rich.List.Sections[0].Rows
				if len(rows) != 1 || rows[0].ID != "r1" || rows[0].Title != "Linha 1" || rows[0].Description != "desc" {
					t.Fatalf("unexpected rows: %+v", rows)
				}
			},
		},
		{
			name:       "cta_url maps to cta flow with button url",
			buttonName: "cta_url",
			paramsJSON: `{"display_text":"Visitar site","url":"https://example.com"}`,
			wantFlow:   "cta",
			check: func(t *testing.T, rich *mapper.InboundRichContent) {
				if len(rich.Buttons) != 1 {
					t.Fatalf("expected 1 button, got %+v", rich.Buttons)
				}
				b := rich.Buttons[0]
				if b.Text != "Visitar site" || b.URL != "https://example.com" {
					t.Fatalf("unexpected button: %+v", b)
				}
			},
		},
		{
			name:       "review_and_pay maps to payment flow",
			buttonName: "review_and_pay",
			paramsJSON: `{"display_text":"Pagar agora"}`,
			wantFlow:   "payment",
			check: func(t *testing.T, rich *mapper.InboundRichContent) {
				if len(rich.Buttons) != 0 {
					t.Fatalf("payment flow must not emit button chips, got %+v", rich.Buttons)
				}
			},
		},
		{
			name:       "payment_info maps to payment flow",
			buttonName: "payment_info",
			paramsJSON: `{"display_text":"Ver cobrança"}`,
			wantFlow:   "payment",
			check: func(t *testing.T, rich *mapper.InboundRichContent) {
				if len(rich.Buttons) != 0 {
					t.Fatalf("payment flow must not emit button chips, got %+v", rich.Buttons)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evt := &events.Message{
				Info: baseInfo("wamid.interactive-native-"+tt.name, "5511999999999"),
				Message: &waE2E.Message{
					InteractiveMessage: &waE2E.InteractiveMessage{
						Body:   &waE2E.InteractiveMessage_Body{Text: proto.String("Interactive body")},
						Footer: &waE2E.InteractiveMessage_Footer{Text: proto.String("Interactive footer")},
						InteractiveMessage: &waE2E.InteractiveMessage_NativeFlowMessage_{
							NativeFlowMessage: &waE2E.InteractiveMessage_NativeFlowMessage{
								Buttons: []*waE2E.InteractiveMessage_NativeFlowMessage_NativeFlowButton{
									{
										Name:             proto.String(tt.buttonName),
										ButtonParamsJSON: proto.String(tt.paramsJSON),
									},
								},
							},
						},
					},
				},
			}

			out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
			if err != nil {
				t.Fatalf("BuildInbound: %v", err)
			}
			if out.Type != "interactive" {
				t.Fatalf("expected Type=interactive, got %q", out.Type)
			}
			if out.RichContent == nil {
				t.Fatalf("expected RichContent, got nil")
			}
			if out.RichContent.Flow != tt.wantFlow {
				t.Fatalf("expected Flow=%q, got %q", tt.wantFlow, out.RichContent.Flow)
			}
			if out.RichContent.Body != "Interactive body" || out.RichContent.Footer != "Interactive footer" {
				t.Fatalf("unexpected body/footer: %+v", out.RichContent)
			}
			tt.check(t, out.RichContent)
		})
	}
}

func paymentInteractiveEvt(id, buttonName, paramsJSON string) *events.Message {
	return &events.Message{
		Info: baseInfo(id, "5511999999999"),
		Message: &waE2E.Message{
			InteractiveMessage: &waE2E.InteractiveMessage{
				InteractiveMessage: &waE2E.InteractiveMessage_NativeFlowMessage_{
					NativeFlowMessage: &waE2E.InteractiveMessage_NativeFlowMessage{
						Buttons: []*waE2E.InteractiveMessage_NativeFlowMessage_NativeFlowButton{
							{Name: proto.String(buttonName), ButtonParamsJSON: proto.String(paramsJSON)},
						},
					},
				},
			},
		},
	}
}

func TestBuildInboundPaymentInfoSharesPixKey(t *testing.T) {
	params := `{"reference_id":"4VSZ0VLQ5GV","payment_settings":[{"type":"pix_static_code","pix_static_code":{"merchant_name":"Eu","key":"+5564984338175","key_type":"PHONE"}}],"currency":"BRL","total_amount":{"value":0,"offset":1000},"order":{"status":"payment_requested","items":[{"quantity":0,"retailer_id":"4VSZ0VLPZ3T","amount":{"offset":1000,"value":0},"name":""}]}}`

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), paymentInteractiveEvt("wamid.pix-key", "payment_info", params))
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	p := out.RichContent.Payment
	if p == nil {
		t.Fatalf("expected Payment, got nil")
	}
	if p.Kind != "key" {
		t.Fatalf("expected Kind=key, got %q", p.Kind)
	}
	if p.PixKey != "+5564984338175" || p.PixKeyType != "PHONE" {
		t.Fatalf("unexpected pix key: %+v", p)
	}
	if p.PixKeyDisplay != "64 98433-8175" {
		t.Fatalf("expected display '64 98433-8175', got %q", p.PixKeyDisplay)
	}
	if p.MerchantName != "Eu" || p.CopyLabel != "Copiar chave Pix" {
		t.Fatalf("unexpected merchant/label: %+v", p)
	}
	if p.Amount != "" {
		t.Fatalf("expected empty amount for key share, got %q", p.Amount)
	}
	if len(p.Items) != 0 {
		t.Fatalf("expected no items for key share, got %+v", p.Items)
	}
}

func TestBuildInboundReviewAndPayIsCharge(t *testing.T) {
	params := `{"reference_id":"4VSZ10LYBWD","currency":"BRL","total_amount":{"value":11000,"offset":1000},"order":{"status":"payment_requested","items":[{"retailer_id":"7493707287308237","name":"teste 3","amount":{"value":11000,"offset":1000},"quantity":1}]},"payment_settings":[{"type":"pix_static_code","pix_static_code":{"merchant_name":"Eu","key":"+5564984338175","key_type":"PHONE"}},{"type":"cards","cards":{"enabled":false}}]}`

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), paymentInteractiveEvt("wamid.pix-charge", "review_and_pay", params))
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	p := out.RichContent.Payment
	if p == nil {
		t.Fatalf("expected Payment, got nil")
	}
	if p.Kind != "charge" || p.ReferenceID != "4VSZ10LYBWD" {
		t.Fatalf("unexpected kind/reference: %+v", p)
	}
	if p.Amount != "R$ 11,00" {
		t.Fatalf("expected Amount='R$ 11,00', got %q", p.Amount)
	}
	if p.PixKey != "+5564984338175" || p.CopyLabel != "Copiar código Pix" {
		t.Fatalf("unexpected pix/label: %+v", p)
	}
	if len(p.Items) != 1 || p.Items[0].Name != "teste 3" || p.Items[0].Quantity != 1 || p.Items[0].Amount != "R$ 11,00" {
		t.Fatalf("unexpected items: %+v", p.Items)
	}
}

func TestBuildInboundReviewOrderIsOrderReceipt(t *testing.T) {
	params := `{"reference_id":"4VSZ10LYBWD","payment_status":"pending","currency":"BRL","total_amount":{"value":11000,"offset":1000},"order":{"status":"delivered","items":[{"retailer_id":"7493707287308237","name":"teste 3","amount":{"value":11000,"offset":1000},"quantity":1}]}}`

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), paymentInteractiveEvt("wamid.pix-order", "review_order", params))
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	p := out.RichContent.Payment
	if p == nil {
		t.Fatalf("expected Payment, got nil")
	}
	if p.Kind != "order" || p.Status != "delivered" {
		t.Fatalf("unexpected kind/status: %+v", p)
	}
	if p.Amount != "R$ 11,00" {
		t.Fatalf("expected Amount='R$ 11,00', got %q", p.Amount)
	}
	if p.PixKey != "" || p.CopyLabel != "" {
		t.Fatalf("order receipt must not carry a pix key/copy button: %+v", p)
	}
	if len(p.Items) != 1 || p.Items[0].Name != "teste 3" {
		t.Fatalf("unexpected items: %+v", p.Items)
	}
}

func TestBuildInboundInteractiveMessagePaymentFlowMissingParamsOmitsPayment(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.interactive-payment-2", "5511999999999"),
		Message: &waE2E.Message{
			InteractiveMessage: &waE2E.InteractiveMessage{
				Body: &waE2E.InteractiveMessage_Body{Text: proto.String("Pagamento via Pix")},
				InteractiveMessage: &waE2E.InteractiveMessage_NativeFlowMessage_{
					NativeFlowMessage: &waE2E.InteractiveMessage_NativeFlowMessage{
						Buttons: []*waE2E.InteractiveMessage_NativeFlowMessage_NativeFlowButton{
							{Name: proto.String("review_and_pay")},
						},
					},
				},
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.RichContent == nil || out.RichContent.Flow != "payment" {
		t.Fatalf("expected Flow=payment, got %+v", out.RichContent)
	}
	if out.RichContent.Payment != nil {
		t.Fatalf("expected no Payment when ButtonParamsJSON is absent, got %+v", out.RichContent.Payment)
	}
}

func TestBuildInboundInteractiveMessageEmptyFallsBackToUnsupported(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.interactive-empty-1", "5511999999999"),
		Message: &waE2E.Message{
			InteractiveMessage: &waE2E.InteractiveMessage{},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "unsupported" {
		t.Fatalf("expected Type=unsupported, got %q", out.Type)
	}
	if out.RichContent != nil {
		t.Fatalf("expected no RichContent for an empty InteractiveMessage, got %+v", out.RichContent)
	}
	if out.Unsupported == nil || out.Unsupported.Type != "interactiveMessage" {
		t.Fatalf("expected Unsupported.Type=interactiveMessage, got %+v", out.Unsupported)
	}
}

func TestBuildInboundProductMessage(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.product-1", "5511999999999"),
		Message: &waE2E.Message{
			ProductMessage: &waE2E.ProductMessage{
				ContextInfo: &waE2E.ContextInfo{StanzaID: proto.String("wamid.quoted-product")},
				Product: &waE2E.ProductMessage_ProductSnapshot{
					Title:           proto.String("Tênis de corrida"),
					Description:     proto.String("Leve e confortável"),
					CurrencyCode:    proto.String("BRL"),
					PriceAmount1000: proto.Int64(129900),
					RetailerID:      proto.String("sku-42"),
				},
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "product" {
		t.Fatalf("expected Type=product, got %q", out.Type)
	}
	if out.ContextMessageID != "wamid.quoted-product" {
		t.Fatalf("expected ContextMessageID=wamid.quoted-product, got %q", out.ContextMessageID)
	}
	if out.RichContent == nil || out.RichContent.Kind != "product" {
		t.Fatalf("expected RichContent.Kind=product, got %+v", out.RichContent)
	}
	product := out.RichContent.Product
	if product == nil || product.Title != "Tênis de corrida" {
		t.Fatalf("expected Product.Title='Tênis de corrida', got %+v", product)
	}
	if product.Description != "Leve e confortável" {
		t.Fatalf("unexpected description: %q", product.Description)
	}
	if product.Currency != "BRL" || product.RetailerID != "sku-42" {
		t.Fatalf("unexpected currency/retailerId: %+v", product)
	}
	if product.PriceText != "R$ 129,90" {
		t.Fatalf("expected PriceText='R$ 129,90', got %q", product.PriceText)
	}
	if product.URL != "" {
		t.Fatalf("expected empty URL when not set, got %q", product.URL)
	}
}

func TestBuildInboundProductMessageWithImageDownloadsAndFormatsPriceAndURL(t *testing.T) {
	dl := fakeDownloader{data: []byte("product image bytes")}
	store := &fakeMediaStore{}
	evt := &events.Message{
		Info: baseInfo("wamid.product-image-1", "5511999999999"),
		Message: &waE2E.Message{
			ProductMessage: &waE2E.ProductMessage{
				Product: &waE2E.ProductMessage_ProductSnapshot{
					Title:           proto.String("Cadeira gamer"),
					CurrencyCode:    proto.String("BRL"),
					PriceAmount1000: proto.Int64(11111000),
					RetailerID:      proto.String("sku-99"),
					URL:             proto.String("https://example.com/product/cadeira-gamer"),
					ProductImage: &waE2E.ImageMessage{
						Mimetype: proto.String("image/jpeg"),
					},
				},
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(dl, nil, store), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "product" {
		t.Fatalf("expected Type=product, got %q", out.Type)
	}
	if out.Media == nil {
		t.Fatalf("expected Media for product with image, got nil")
	}
	if out.Media.Key == "" {
		t.Fatalf("expected non-empty media key")
	}
	if out.Media.MimeType != "image/jpeg" {
		t.Fatalf("expected MimeType=image/jpeg, got %q", out.Media.MimeType)
	}
	if len(store.puts) != 1 || string(store.puts[0].data) != "product image bytes" {
		t.Fatalf("expected product image bytes stored, got %+v", store.puts)
	}
	if out.RichContent == nil || out.RichContent.Product == nil {
		t.Fatalf("expected RichContent.Product, got %+v", out.RichContent)
	}
	if out.RichContent.Product.PriceText != "R$ 11.111,00" {
		t.Fatalf("expected PriceText='R$ 11.111,00', got %q", out.RichContent.Product.PriceText)
	}
	if out.RichContent.Product.URL != "https://example.com/product/cadeira-gamer" {
		t.Fatalf("expected Product.URL set, got %q", out.RichContent.Product.URL)
	}
}

func TestBuildInboundProductMessageImageDownloadFailureStillEmitsRichContent(t *testing.T) {
	dl := fakeDownloader{err: errors.New("download failed")}
	store := &fakeMediaStore{}
	evt := &events.Message{
		Info: baseInfo("wamid.product-image-err-1", "5511999999999"),
		Message: &waE2E.Message{
			ProductMessage: &waE2E.ProductMessage{
				Product: &waE2E.ProductMessage_ProductSnapshot{
					Title:           proto.String("Cadeira gamer"),
					CurrencyCode:    proto.String("BRL"),
					PriceAmount1000: proto.Int64(129900),
					ProductImage: &waE2E.ImageMessage{
						Mimetype: proto.String("image/jpeg"),
					},
				},
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(dl, nil, store), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "product" {
		t.Fatalf("expected Type=product, got %q", out.Type)
	}
	if out.Media != nil {
		t.Fatalf("expected no Media when image download fails, got %+v", out.Media)
	}
	if out.RichContent == nil || out.RichContent.Product == nil || out.RichContent.Product.Title != "Cadeira gamer" {
		t.Fatalf("expected RichContent.Product still emitted, got %+v", out.RichContent)
	}
	if len(store.puts) != 0 {
		t.Fatalf("expected no store.Put calls after download failure, got %d", len(store.puts))
	}
}

func TestBuildInboundProductMessageWithoutTitleFallsBackToUnsupported(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.product-empty-1", "5511999999999"),
		Message: &waE2E.Message{
			ProductMessage: &waE2E.ProductMessage{
				Product: &waE2E.ProductMessage_ProductSnapshot{},
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "unsupported" {
		t.Fatalf("expected Type=unsupported, got %q", out.Type)
	}
	if out.RichContent != nil {
		t.Fatalf("expected no RichContent for a titleless ProductMessage, got %+v", out.RichContent)
	}
	if out.Unsupported == nil || out.Unsupported.Type != "productMessage" {
		t.Fatalf("expected Unsupported.Type=productMessage, got %+v", out.Unsupported)
	}
}

func orderEvent(thumbnail []byte) *events.Message {
	return &events.Message{
		Info: baseInfo("wamid.order-1", "5511999999999"),
		Message: &waE2E.Message{
			OrderMessage: &waE2E.OrderMessage{
				OrderID:           proto.String("2286705342067430"),
				ItemCount:         proto.Int32(1),
				Status:            waE2E.OrderMessage_INQUIRY.Enum(),
				OrderTitle:        proto.String(""),
				SellerJID:         proto.String("556484338175@s.whatsapp.net"),
				TotalAmount1000:   proto.Int64(11111000),
				TotalCurrencyCode: proto.String("BRL"),
				Thumbnail:         thumbnail,
			},
		},
	}
}

func TestBuildInboundOrder(t *testing.T) {
	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), orderEvent(nil))
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "order" {
		t.Fatalf("expected Type=order, got %q", out.Type)
	}
	if out.RichContent == nil || out.RichContent.Kind != "order" {
		t.Fatalf("expected RichContent.Kind=order, got %+v", out.RichContent)
	}
	order := out.RichContent.Order
	if order == nil || order.OrderID != "2286705342067430" {
		t.Fatalf("expected Order.OrderID=2286705342067430, got %+v", order)
	}
	if order.ItemCount != 1 {
		t.Fatalf("expected Order.ItemCount=1, got %d", order.ItemCount)
	}
	if order.Status != "INQUIRY" {
		t.Fatalf("expected Order.Status=INQUIRY, got %q", order.Status)
	}
	if order.SellerJID != "556484338175@s.whatsapp.net" {
		t.Fatalf("unexpected Order.SellerJID: %q", order.SellerJID)
	}
	if order.Currency != "BRL" {
		t.Fatalf("expected Order.Currency=BRL, got %q", order.Currency)
	}
	if order.AmountText != "R$ 11.111,00" {
		t.Fatalf("expected Order.AmountText='R$ 11.111,00', got %q", order.AmountText)
	}
}

func TestBuildInboundOrderStoresTheThumbnail(t *testing.T) {
	store := &fakeMediaStore{}

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, store), orderEvent([]byte("jpeg-bytes")))
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Media == nil {
		t.Fatalf("expected Media for an order with a thumbnail, got nil")
	}
	if out.Media.Key != "inbound-media/tenant-1/wamid.order-1" {
		t.Fatalf("expected Media.Key=inbound-media/tenant-1/wamid.order-1, got %q", out.Media.Key)
	}
	if out.Media.MimeType != "image/jpeg" {
		t.Fatalf("expected MimeType=image/jpeg, got %q", out.Media.MimeType)
	}
	if len(store.puts) != 1 {
		t.Fatalf("expected one stored object, got %d", len(store.puts))
	}
	if store.puts[0].key != out.Media.Key || store.puts[0].mime != "image/jpeg" {
		t.Fatalf("unexpected stored object: %+v", store.puts[0])
	}
	if string(store.puts[0].data) != "jpeg-bytes" {
		t.Fatalf("expected the thumbnail bytes stored, got %q", store.puts[0].data)
	}
}

func TestBuildInboundOrderWithoutThumbnailHasNoMedia(t *testing.T) {
	store := &fakeMediaStore{}

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, store), orderEvent(nil))
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Media != nil {
		t.Fatalf("expected no Media for an order without a thumbnail, got %+v", out.Media)
	}
	if len(store.puts) != 0 {
		t.Fatalf("expected no store.Put calls, got %d", len(store.puts))
	}
}

func TestBuildInboundOrderWithoutAnIDIsUnsupported(t *testing.T) {
	evt := orderEvent(nil)
	evt.Message.OrderMessage.OrderID = nil

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "unsupported" {
		t.Fatalf("expected Type=unsupported, got %q", out.Type)
	}
	if out.RichContent != nil {
		t.Fatalf("expected no RichContent for an OrderMessage without an ID, got %+v", out.RichContent)
	}
	if out.Unsupported == nil || out.Unsupported.Type != "orderMessage" {
		t.Fatalf("expected Unsupported.Type=orderMessage, got %+v", out.Unsupported)
	}
}

func TestBuildInboundEventMessage(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.event-1", "5511999999999"),
		Message: &waE2E.Message{
			EventMessage: &waE2E.EventMessage{
				ContextInfo: &waE2E.ContextInfo{StanzaID: proto.String("wamid.quoted-event")},
				Name:        proto.String("Lançamento de produto"),
				Description: proto.String("Venha conhecer as novidades"),
				StartTime:   proto.Int64(1700000000),
				EndTime:     proto.Int64(1700003600),
				JoinLink:    proto.String("https://example.com/join"),
				IsCanceled:  proto.Bool(false),
				Location:    &waE2E.LocationMessage{Name: proto.String("Sede vectax")},
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "event" {
		t.Fatalf("expected Type=event, got %q", out.Type)
	}
	if out.ContextMessageID != "wamid.quoted-event" {
		t.Fatalf("expected ContextMessageID=wamid.quoted-event, got %q", out.ContextMessageID)
	}
	if out.RichContent == nil || out.RichContent.Kind != "event" {
		t.Fatalf("expected RichContent.Kind=event, got %+v", out.RichContent)
	}
	event := out.RichContent.Event
	if event == nil || event.Name != "Lançamento de produto" {
		t.Fatalf("expected Event.Name='Lançamento de produto', got %+v", event)
	}
	if event.Description != "Venha conhecer as novidades" {
		t.Fatalf("unexpected description: %q", event.Description)
	}
	if event.StartTime != "2023-11-14T22:13:20Z" || event.EndTime != "2023-11-14T23:13:20Z" {
		t.Fatalf("unexpected start/end time: %+v", event)
	}
	if event.Location != "Sede vectax" {
		t.Fatalf("unexpected location: %q", event.Location)
	}
	if event.JoinLink != "https://example.com/join" {
		t.Fatalf("unexpected join link: %q", event.JoinLink)
	}
	if event.Canceled {
		t.Fatalf("expected Canceled=false, got true")
	}
}

func TestBuildInboundEventMessageCanceled(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.event-2", "5511999999999"),
		Message: &waE2E.Message{
			EventMessage: &waE2E.EventMessage{
				Name:       proto.String("Reunião cancelada"),
				IsCanceled: proto.Bool(true),
			},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.RichContent == nil || out.RichContent.Event == nil {
		t.Fatalf("expected RichContent.Event, got %+v", out.RichContent)
	}
	if !out.RichContent.Event.Canceled {
		t.Fatalf("expected Canceled=true, got false")
	}
}

func TestBuildInboundEventMessageWithoutNameFallsBackToUnsupported(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.event-empty-1", "5511999999999"),
		Message: &waE2E.Message{
			EventMessage: &waE2E.EventMessage{},
		},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "unsupported" {
		t.Fatalf("expected Type=unsupported, got %q", out.Type)
	}
	if out.RichContent != nil {
		t.Fatalf("expected no RichContent for a nameless EventMessage, got %+v", out.RichContent)
	}
	if out.Unsupported == nil || out.Unsupported.Type != "eventMessage" {
		t.Fatalf("expected Unsupported.Type=eventMessage, got %+v", out.Unsupported)
	}
}

func TestBuildInboundDebugRawDumpDisabledLeavesTextMessageUnchanged(t *testing.T) {
	evt := &events.Message{
		Info:    baseInfo("wamid.debug-off-1", "5511999999999"),
		Message: &waE2E.Message{Conversation: proto.String("hello with debug off")},
	}

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "text" {
		t.Fatalf("expected Type=text, got %q", out.Type)
	}
	if out.Text == nil || out.Text.Body != "hello with debug off" {
		t.Fatalf("expected Text.Body='hello with debug off', got %+v", out.Text)
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

func TestBuildInboundNamesTheReactorEvenOnOurOwnGroupReaction(t *testing.T) {
	for _, fromMe := range []bool{false, true} {
		evt := &events.Message{
			Info: types.MessageInfo{
				MessageSource: types.MessageSource{
					Chat:     types.NewJID("120363000000000000", types.GroupServer),
					Sender:   types.NewJID("173907587899617", types.HiddenUserServer),
					IsFromMe: fromMe,
					IsGroup:  true,
				},
				ID:        "REACT1",
				Timestamp: time.Unix(1700000000, 0),
			},
			Message: &waE2E.Message{ReactionMessage: &waE2E.ReactionMessage{
				Key:  &waCommon.MessageKey{ID: proto.String("TARGET1")},
				Text: proto.String("🙏"),
			}},
		}

		out, err := mapper.BuildInbound(context.Background(), mapper.InboundDeps{}, evt)
		if err != nil {
			t.Fatalf("fromMe=%v: %v", fromMe, err)
		}
		if out.Reaction == nil {
			t.Fatalf("fromMe=%v: expected a reaction", fromMe)
		}
		if out.Reaction.SenderJID != "173907587899617@lid" {
			t.Fatalf("fromMe=%v: senderJid = %q", fromMe, out.Reaction.SenderJID)
		}
	}
}

func TestBuildInboundOrderWithoutAStatusOmitsIt(t *testing.T) {
	evt := orderEvent(nil)
	evt.Message.OrderMessage.Status = nil

	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.RichContent == nil || out.RichContent.Order == nil {
		t.Fatalf("expected an order rich content, got %+v", out.RichContent)
	}
	if got := out.RichContent.Order.Status; got != "" {
		t.Fatalf("expected no status when the field is absent, got %q", got)
	}

	raw, err := json.Marshal(out.RichContent.Order)
	if err != nil {
		t.Fatalf("marshal order: %v", err)
	}
	if strings.Contains(string(raw), `"status"`) {
		t.Fatalf("expected the status key to be absent from the wire payload, got %s", raw)
	}
}
