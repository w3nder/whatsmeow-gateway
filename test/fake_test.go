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

	connectCalls int

	sendResp whatsmeow.SendResponse
	sendErr  error

	handlers []func(any)
}

var _ session.WAClient = (*fakeWAClient)(nil)

func newFakeWAClient() *fakeWAClient {
	return &fakeWAClient{}
}

func (f *fakeWAClient) QRChannel(ctx context.Context) (<-chan whatsmeow.QRChannelItem, error) {
	f.mu.Lock()
	items := f.qrItems
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

func (f *fakeWAClient) SendMessage(ctx context.Context, to types.JID, msg *waE2E.Message) (whatsmeow.SendResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
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

func (f *fakeWAClient) Disconnect() {}

func (f *fakeWAClient) emit(evt any) {
	f.mu.Lock()
	handlers := append([]func(any){}, f.handlers...)
	f.mu.Unlock()

	for _, h := range handlers {
		h(evt)
	}
}
