package session

import (
	"context"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
)

type WAClient interface {
	QRChannel(ctx context.Context) (<-chan whatsmeow.QRChannelItem, error)
	Connect() error
	IsLoggedIn() bool
	DeviceJID() *types.JID
	SendMessage(ctx context.Context, to types.JID, msg *waE2E.Message) (whatsmeow.SendResponse, error)
	Upload(ctx context.Context, data []byte, mt whatsmeow.MediaType) (whatsmeow.UploadResponse, error)
	Download(ctx context.Context, msg whatsmeow.DownloadableMessage) ([]byte, error)
	AddEventHandler(handler func(any)) uint32
	Disconnect()
}

type waClient struct {
	client *whatsmeow.Client
}

func NewWAClient(device *store.Device, log waLog.Logger) WAClient {
	client := whatsmeow.NewClient(device, log)
	client.EnableAutoReconnect = true
	return &waClient{client: client}
}

func (w *waClient) QRChannel(ctx context.Context) (<-chan whatsmeow.QRChannelItem, error) {
	return w.client.GetQRChannel(ctx)
}

func (w *waClient) Connect() error {
	return w.client.Connect()
}

func (w *waClient) IsLoggedIn() bool {
	return w.client.IsLoggedIn()
}

func (w *waClient) DeviceJID() *types.JID {
	return w.client.Store.ID
}

func (w *waClient) SendMessage(ctx context.Context, to types.JID, msg *waE2E.Message) (whatsmeow.SendResponse, error) {
	return w.client.SendMessage(ctx, to, msg)
}

func (w *waClient) Upload(ctx context.Context, data []byte, mt whatsmeow.MediaType) (whatsmeow.UploadResponse, error) {
	return w.client.Upload(ctx, data, mt)
}

func (w *waClient) Download(ctx context.Context, msg whatsmeow.DownloadableMessage) ([]byte, error) {
	return w.client.Download(ctx, msg)
}

func (w *waClient) AddEventHandler(handler func(any)) uint32 {
	return w.client.AddEventHandler(handler)
}

func (w *waClient) Disconnect() {
	w.client.Disconnect()
}
