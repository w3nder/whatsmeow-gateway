package mapper_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/w3nder/whatsmeow-gateway/internal/avatar"
	"github.com/w3nder/whatsmeow-gateway/internal/mapper"
)

type fakeAvatars struct {
	pic  *avatar.Picture
	jids []types.JID
}

func (f *fakeAvatars) For(ctx context.Context, jid types.JID) *avatar.Picture {
	f.jids = append(f.jids, jid)
	return f.pic
}

func storedPicture() *avatar.Picture {
	return &avatar.Picture{
		Key:      "profile-pictures/tenant-1/5511999999999/pic-1",
		ID:       "pic-1",
		MimeType: "image/jpeg",
	}
}

func withAvatars(avatars mapper.AvatarSource) mapper.InboundDeps {
	d := testDeps(fakeDownloader{}, nil, &fakeMediaStore{})
	d.Avatars = avatars
	return d
}

func TestBuildInboundCarriesTheSenderProfilePicture(t *testing.T) {
	evt := &events.Message{
		Info:    baseInfo("wamid.pic-1", "5511999999999"),
		Message: &waE2E.Message{Conversation: proto.String("hello there")},
	}
	avatars := &fakeAvatars{pic: storedPicture()}

	out, err := mapper.BuildInbound(context.Background(), withAvatars(avatars), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}

	if out.ProfilePicture == nil {
		t.Fatal("expected a profile picture, got nil")
	}
	if *out.ProfilePicture != *storedPicture() {
		t.Errorf("profile picture = %+v, want %+v", out.ProfilePicture, storedPicture())
	}
	if len(avatars.jids) != 1 {
		t.Fatalf("avatar lookups = %d, want 1", len(avatars.jids))
	}
	if got := avatars.jids[0].User; got != "5511999999999" {
		t.Errorf("looked up %q, want the sender 5511999999999", got)
	}
}

func TestBuildInboundLooksUpTheLIDSenderProfilePicture(t *testing.T) {
	evt := &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Sender:    types.NewJID("173907587899617", types.HiddenUserServer),
				SenderAlt: types.NewJID("5511988887777", types.DefaultUserServer),
			},
			ID:        "wamid.pic-2",
			Timestamp: time.Unix(1700000000, 0),
		},
		Message: &waE2E.Message{Conversation: proto.String("hello there")},
	}
	avatars := &fakeAvatars{pic: storedPicture()}

	if _, err := mapper.BuildInbound(context.Background(), withAvatars(avatars), evt); err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}

	if len(avatars.jids) != 1 {
		t.Fatalf("avatar lookups = %d, want 1", len(avatars.jids))
	}
	if got := avatars.jids[0]; got.User != "173907587899617" || got.Server != types.HiddenUserServer {
		t.Errorf("looked up %s, want the @lid sender 173907587899617", got)
	}
}

func TestBuildInboundDoesNotLookUpAProfilePictureForOurOwnMessage(t *testing.T) {
	info := baseInfo("wamid.pic-3", "5511999999999")
	info.IsFromMe = true
	info.Chat = types.NewJID("5511999999999", types.DefaultUserServer)
	evt := &events.Message{
		Info:    info,
		Message: &waE2E.Message{Conversation: proto.String("hello there")},
	}
	avatars := &fakeAvatars{pic: storedPicture()}

	out, err := mapper.BuildInbound(context.Background(), withAvatars(avatars), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}

	if out.ProfilePicture != nil {
		t.Errorf("profile picture = %+v, want nil", out.ProfilePicture)
	}
	if len(avatars.jids) != 0 {
		t.Errorf("avatar lookups = %d, want 0", len(avatars.jids))
	}
}

func TestBuildInboundSkipsTheProfilePictureWhenTheContactHasNone(t *testing.T) {
	evt := &events.Message{
		Info:    baseInfo("wamid.pic-4", "5511999999999"),
		Message: &waE2E.Message{Conversation: proto.String("hello there")},
	}

	out, err := mapper.BuildInbound(context.Background(), withAvatars(&fakeAvatars{pic: nil}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}

	if out.ProfilePicture != nil {
		t.Errorf("profile picture = %+v, want nil", out.ProfilePicture)
	}
	raw, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(raw), "profilePicture") {
		t.Errorf("event JSON carries an empty profilePicture: %s", raw)
	}
}

func TestBuildInboundWithoutAnAvatarSourceStillMapsTheMessage(t *testing.T) {
	evt := &events.Message{
		Info:    baseInfo("wamid.pic-5", "5511999999999"),
		Message: &waE2E.Message{Conversation: proto.String("hello there")},
	}

	out, err := mapper.BuildInbound(context.Background(), withAvatars(nil), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}

	if out.ProfilePicture != nil {
		t.Errorf("profile picture = %+v, want nil", out.ProfilePicture)
	}
	if out.Text == nil || out.Text.Body != "hello there" {
		t.Errorf("text = %+v, want the message body", out.Text)
	}
}

func TestBuildInboundSerializesTheProfilePicture(t *testing.T) {
	evt := &events.Message{
		Info:    baseInfo("wamid.pic-6", "5511999999999"),
		Message: &waE2E.Message{Conversation: proto.String("hello there")},
	}

	out, err := mapper.BuildInbound(context.Background(), withAvatars(&fakeAvatars{pic: storedPicture()}), evt)
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}

	raw, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded struct {
		ProfilePicture struct {
			Key      string `json:"key"`
			ID       string `json:"id"`
			MimeType string `json:"mimeType"`
		} `json:"profilePicture"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.ProfilePicture.Key != "profile-pictures/tenant-1/5511999999999/pic-1" {
		t.Errorf("profilePicture.key = %q", decoded.ProfilePicture.Key)
	}
	if decoded.ProfilePicture.ID != "pic-1" {
		t.Errorf("profilePicture.id = %q", decoded.ProfilePicture.ID)
	}
	if decoded.ProfilePicture.MimeType != "image/jpeg" {
		t.Errorf("profilePicture.mimeType = %q", decoded.ProfilePicture.MimeType)
	}
}

func TestBuildInboundDoesNotLookUpAProfilePictureForAnUnmappableMessage(t *testing.T) {
	evt := &events.Message{
		Info: baseInfo("wamid.pic-7", "5511999999999"),
		Message: &waE2E.Message{
			ProtocolMessage: &waE2E.ProtocolMessage{Type: waE2E.ProtocolMessage_APP_STATE_SYNC_KEY_SHARE.Enum()},
		},
	}
	avatars := &fakeAvatars{pic: storedPicture()}

	if _, err := mapper.BuildInbound(context.Background(), withAvatars(avatars), evt); err == nil {
		t.Fatal("expected ErrSkip for a protocol message")
	}

	if len(avatars.jids) != 0 {
		t.Errorf("avatar lookups = %d, want 0", len(avatars.jids))
	}
}
