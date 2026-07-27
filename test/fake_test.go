package test

import (
	"context"
	"sync"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"

	"github.com/w3nder/whatsmeow-gateway/internal/session"
)

type fakeWAClient struct {
	mu sync.Mutex

	deviceJID *types.JID
	loggedIn  bool

	qrItems    []whatsmeow.QRChannelItem
	connectErr error

	connectCalls   int
	qrChannelCalls int

	sendResp  whatsmeow.SendResponse
	sendErr   error
	sendCalls int
	lastTo    types.JID
	lastMsg   *waE2E.Message
	lastID    types.MessageID

	disconnectCalls int

	handlers []func(any)
}

var _ session.WAClient = (*fakeWAClient)(nil)

func newFakeWAClient() *fakeWAClient {
	return &fakeWAClient{}
}

func (f *fakeWAClient) QRChannel(ctx context.Context) (<-chan whatsmeow.QRChannelItem, error) {
	f.mu.Lock()
	f.qrChannelCalls++
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
	err := f.connectErr
	f.mu.Unlock()
	return err
}

func (f *fakeWAClient) IsLoggedIn() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.loggedIn
}

func (f *fakeWAClient) DeviceJID() *types.JID {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deviceJID
}

func (f *fakeWAClient) SendMessage(ctx context.Context, to types.JID, msg *waE2E.Message, id types.MessageID) (whatsmeow.SendResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sendCalls++
	f.lastTo = to
	f.lastMsg = msg
	f.lastID = id
	return f.sendResp, f.sendErr
}

func (f *fakeWAClient) Upload(ctx context.Context, data []byte, mt whatsmeow.MediaType) (whatsmeow.UploadResponse, error) {
	return whatsmeow.UploadResponse{}, nil
}

func (f *fakeWAClient) Download(ctx context.Context, msg whatsmeow.DownloadableMessage) ([]byte, error) {
	return nil, nil
}

func (f *fakeWAClient) AddEventHandler(handler func(any)) uint32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.handlers = append(f.handlers, handler)
	return uint32(len(f.handlers))
}

func (f *fakeWAClient) Disconnect() {
	f.mu.Lock()
	f.disconnectCalls++
	f.mu.Unlock()
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
