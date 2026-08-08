package session

import (
	"context"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/w3nder/whatsmeow-gateway/internal/call"
)

type recordingClient struct {
	lastExtra whatsmeow.SendRequestExtra
}

func (f *recordingClient) QRChannel(ctx context.Context) (<-chan whatsmeow.QRChannelItem, error) {
	return nil, nil
}

func (f *recordingClient) Connect() error {
	return nil
}

func (f *recordingClient) IsLoggedIn() bool {
	return false
}

func (f *recordingClient) IsConnected() bool {
	return false
}

func (f *recordingClient) WaitForConnection(timeout time.Duration) bool {
	return false
}

func (f *recordingClient) DeviceJID() *types.JID {
	return nil
}

func (f *recordingClient) SendMessage(ctx context.Context, to types.JID, msg *waE2E.Message, id types.MessageID, nodes []waBinary.Node) (whatsmeow.SendResponse, error) {
	f.lastExtra = sendExtra(id, nodes)
	return whatsmeow.SendResponse{}, nil
}

func (f *recordingClient) BuildEdit(chat types.JID, id types.MessageID, newContent *waE2E.Message) *waE2E.Message {
	return newContent
}

func (f *recordingClient) BuildRevoke(chat, sender types.JID, id types.MessageID) *waE2E.Message {
	return &waE2E.Message{}
}

func (f *recordingClient) BuildReaction(chat, sender types.JID, id types.MessageID, reaction string) *waE2E.Message {
	return &waE2E.Message{}
}

func (f *recordingClient) Upload(ctx context.Context, data []byte, mt whatsmeow.MediaType) (whatsmeow.UploadResponse, error) {
	return whatsmeow.UploadResponse{}, nil
}

func (f *recordingClient) Download(ctx context.Context, msg whatsmeow.DownloadableMessage) ([]byte, error) {
	return nil, nil
}

func (f *recordingClient) PNForLID(ctx context.Context, lid types.JID) (types.JID, bool, error) {
	return types.JID{}, false, nil
}

func (f *recordingClient) DecryptSecretEncryptedMessage(ctx context.Context, evt *events.Message) (*waE2E.Message, error) {
	return nil, nil
}

func (f *recordingClient) DecryptPollVote(ctx context.Context, evt *events.Message) (*waE2E.PollVoteMessage, error) {
	return nil, nil
}

func (f *recordingClient) GetProfilePictureInfo(ctx context.Context, jid types.JID, params *whatsmeow.GetProfilePictureParams) (*types.ProfilePictureInfo, error) {
	return nil, nil
}

func (f *recordingClient) GetGroupInfo(ctx context.Context, jid types.JID) (*types.GroupInfo, error) {
	return nil, nil
}

func (f *recordingClient) AddEventHandler(handler func(any)) uint32 {
	return 0
}

func (f *recordingClient) Calls() call.Caller {
	return nil
}

func (f *recordingClient) Disconnect() {}

var _ WAClient = (*recordingClient)(nil)

func TestSendPassesAdditionalNodesThrough(t *testing.T) {
	fake := &recordingClient{}
	nodes := []waBinary.Node{{Tag: "biz"}}

	if _, err := fake.SendMessage(context.Background(), types.NewJID("5511999998888", types.DefaultUserServer), &waE2E.Message{}, "MSG1", nodes); err != nil {
		t.Fatal(err)
	}

	if fake.lastExtra.AdditionalNodes == nil {
		t.Fatal("additional nodes were dropped before reaching whatsmeow")
	}
	if len(*fake.lastExtra.AdditionalNodes) != 1 || (*fake.lastExtra.AdditionalNodes)[0].Tag != "biz" {
		t.Fatalf("nodes = %+v", *fake.lastExtra.AdditionalNodes)
	}
	if fake.lastExtra.ID != "MSG1" {
		t.Fatalf("id = %q, want MSG1", fake.lastExtra.ID)
	}
}

func TestSendWithoutNodesKeepsTodaysExtra(t *testing.T) {
	fake := &recordingClient{}

	if _, err := fake.SendMessage(context.Background(), types.NewJID("5511999998888", types.DefaultUserServer), &waE2E.Message{}, "MSG2", nil); err != nil {
		t.Fatal(err)
	}

	if fake.lastExtra.AdditionalNodes != nil {
		t.Fatal("a plain send must not carry additional nodes")
	}
}
