package test

import (
	"context"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/w3nder/whatsmeow-gateway/internal/call"
	"github.com/w3nder/whatsmeow-gateway/internal/session"
)

type fakeWAClient struct {
	mu sync.Mutex

	deviceJID *types.JID
	loggedIn  bool
	connected bool

	// staysDown keeps the fake disconnected after Connect, standing in for a socket
	// that whatsmeow is still retrying in the background.
	staysDown bool

	qrItems []whatsmeow.QRChannelItem
	// qrFeed, when set, is handed out by QRChannel as-is, so a test can hold a pairing
	// session open and decide when each item lands. qrItems stays for the tests that
	// only need a fixed script played out at once.
	qrFeed     chan whatsmeow.QRChannelItem
	connectErr error

	connectCalls      int
	qrChannelCalls    int
	waitCalls         int
	handlersAtConnect int

	sendResp  whatsmeow.SendResponse
	sendErr   error
	sendCalls int
	lastTo    types.JID
	lastMsg   *waE2E.Message
	lastID    types.MessageID
	lastNodes []waBinary.Node

	disconnectCalls int

	handlers []func(any)

	// caller stands in for the channel's calling client; nil unless a test wires one.
	caller call.Caller
}

var _ session.WAClient = (*fakeWAClient)(nil)

func newFakeWAClient() *fakeWAClient {
	return &fakeWAClient{}
}

// QRChannel mirrors whatsmeow's GetQRChannel, refusal included: a client whose socket
// is up gets no QR channel at all. Reusing a client that already went through the QR
// flow is exactly the case that refusal catches, so the fake has to reproduce it.
func (f *fakeWAClient) QRChannel(ctx context.Context) (<-chan whatsmeow.QRChannelItem, error) {
	f.mu.Lock()
	f.qrChannelCalls++
	if f.connected {
		f.mu.Unlock()
		return nil, whatsmeow.ErrQRAlreadyConnected
	}
	if f.qrFeed != nil {
		feed := f.qrFeed
		f.mu.Unlock()
		return feed, nil
	}
	items := f.qrItems
	if f.deviceJID == nil {
		for _, item := range items {
			if item == whatsmeow.QRChannelSuccess {
				jid := types.NewJID("15550000000", types.DefaultUserServer)
				f.deviceJID = &jid
				break
			}
		}
	}
	f.mu.Unlock()

	ch := make(chan whatsmeow.QRChannelItem, len(items))
	for _, item := range items {
		ch <- item
	}
	close(ch)
	return ch, nil
}

func (f *fakeWAClient) Connect() error {
	f.mu.Lock()
	f.connectCalls++
	f.handlersAtConnect = len(f.handlers)
	err := f.connectErr
	if err == nil && !f.staysDown {
		f.connected = true
		f.loggedIn = true
	}
	f.mu.Unlock()
	return err
}

func (f *fakeWAClient) IsLoggedIn() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.loggedIn
}

func (f *fakeWAClient) IsConnected() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connected
}

func (f *fakeWAClient) WaitForConnection(time.Duration) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.waitCalls++
	return f.connected && f.loggedIn
}

func (f *fakeWAClient) DeviceJID() *types.JID {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deviceJID
}

func (f *fakeWAClient) SendMessage(ctx context.Context, to types.JID, msg *waE2E.Message, id types.MessageID, nodes []waBinary.Node) (whatsmeow.SendResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sendCalls++
	f.lastTo = to
	f.lastMsg = msg
	f.lastID = id
	f.lastNodes = nodes
	return f.sendResp, f.sendErr
}

func (f *fakeWAClient) BuildEdit(chat types.JID, id types.MessageID, newContent *waE2E.Message) *waE2E.Message {
	return newContent
}

func (f *fakeWAClient) BuildRevoke(chat, sender types.JID, id types.MessageID) *waE2E.Message {
	return &waE2E.Message{}
}

func (f *fakeWAClient) BuildReaction(chat, sender types.JID, id types.MessageID, reaction string) *waE2E.Message {
	return &waE2E.Message{
		ReactionMessage: &waE2E.ReactionMessage{
			Key:  &waCommon.MessageKey{ID: proto.String(string(id))},
			Text: proto.String(reaction),
		},
	}
}

func (f *fakeWAClient) Upload(ctx context.Context, data []byte, mt whatsmeow.MediaType) (whatsmeow.UploadResponse, error) {
	return whatsmeow.UploadResponse{}, nil
}

func (f *fakeWAClient) Download(ctx context.Context, msg whatsmeow.DownloadableMessage) ([]byte, error) {
	return nil, nil
}

func (f *fakeWAClient) PNForLID(ctx context.Context, lid types.JID) (types.JID, bool, error) {
	return types.JID{}, false, nil
}

func (f *fakeWAClient) DecryptSecretEncryptedMessage(ctx context.Context, evt *events.Message) (*waE2E.Message, error) {
	return nil, nil
}

func (f *fakeWAClient) DecryptPollVote(ctx context.Context, evt *events.Message) (*waE2E.PollVoteMessage, error) {
	return nil, nil
}

// GetProfilePictureInfo answers as a contact with no photo does. These
// end-to-end tests assert on the events a channel publishes, and a profile
// photo is decoration on those events rather than part of their shape.
func (f *fakeWAClient) GetProfilePictureInfo(ctx context.Context, jid types.JID, params *whatsmeow.GetProfilePictureParams) (*types.ProfilePictureInfo, error) {
	return nil, whatsmeow.ErrProfilePictureNotSet
}

func (f *fakeWAClient) GetGroupInfo(ctx context.Context, jid types.JID) (*types.GroupInfo, error) {
	return nil, whatsmeow.ErrIQTimedOut
}

func (f *fakeWAClient) AddEventHandler(handler func(any)) uint32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.handlers = append(f.handlers, handler)
	return uint32(len(f.handlers))
}

func (f *fakeWAClient) Calls() call.Caller {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.caller
}

func (f *fakeWAClient) Disconnect() {
	f.mu.Lock()
	f.disconnectCalls++
	f.connected = false
	f.mu.Unlock()
}

// dropSocket simulates the network dying under the client: the socket is gone but
// whatsmeow still reports the device as logged in, because it only clears that flag
// on a stream error.
func (f *fakeWAClient) dropSocket() {
	f.mu.Lock()
	f.connected = false
	f.loggedIn = true
	f.mu.Unlock()
}

// markPaired stands in for whatsmeow writing the device's JID to the store on
// PairSuccess, which a test feeding items through qrFeed has to do for itself.
func (f *fakeWAClient) markPaired() {
	jid := types.NewJID("15550000000", types.DefaultUserServer)
	f.mu.Lock()
	f.deviceJID = &jid
	f.mu.Unlock()
}

func (f *fakeWAClient) handlerCountAtConnect() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.handlersAtConnect
}

func (f *fakeWAClient) disconnectCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.disconnectCalls
}

func (f *fakeWAClient) sendCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sendCalls
}

func (f *fakeWAClient) qrChannelCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.qrChannelCalls
}

func (f *fakeWAClient) connectCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connectCalls
}

func (f *fakeWAClient) emit(evt any) {
	f.mu.Lock()
	handlers := append([]func(any){}, f.handlers...)
	f.mu.Unlock()

	for _, h := range handlers {
		h(evt)
	}
}
