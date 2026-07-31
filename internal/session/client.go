package session

import (
	"context"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// MaxAutoReconnectDelay caps the gap between reconnect attempts. whatsmeow sleeps
// AutoReconnectErrors*2s before each retry and never bounds it, so a long outage would
// push attempts hours apart and a channel would look dead well after the network came
// back. whatsmeow resets the counter on every successful connection.
const MaxAutoReconnectDelay = 5 * time.Minute

const maxAutoReconnectErrors = int(MaxAutoReconnectDelay / (2 * time.Second))

type WAClient interface {
	QRChannel(ctx context.Context) (<-chan whatsmeow.QRChannelItem, error)
	Connect() error
	IsLoggedIn() bool
	IsConnected() bool
	WaitForConnection(timeout time.Duration) bool
	DeviceJID() *types.JID
	SendMessage(ctx context.Context, to types.JID, msg *waE2E.Message, id types.MessageID) (whatsmeow.SendResponse, error)
	Upload(ctx context.Context, data []byte, mt whatsmeow.MediaType) (whatsmeow.UploadResponse, error)
	Download(ctx context.Context, msg whatsmeow.DownloadableMessage) ([]byte, error)
	PNForLID(ctx context.Context, lid types.JID) (types.JID, bool, error)
	AddEventHandler(handler func(any)) uint32
	Disconnect()
}

type waClient struct {
	client *whatsmeow.Client
}

func NewWAClient(device *store.Device, log waLog.Logger) WAClient {
	client := whatsmeow.NewClient(device, log)
	ConfigureAutoReconnect(client)
	return &waClient{client: client}
}

// ConfigureAutoReconnect turns on whatsmeow's socket recovery for a paired device.
//
// EnableAutoReconnect alone only covers sockets that die after a successful connect:
// whatsmeow returns a retryable network error straight from Connect unless
// InitialAutoReconnect is set, which would leave a channel down for good whenever the
// network happened to be unstable at boot or resume time. The hook keeps retrying
// indefinitely (a flaky network must never require re-pairing) with a bounded backoff.
func ConfigureAutoReconnect(client *whatsmeow.Client) {
	client.EnableAutoReconnect = true
	client.InitialAutoReconnect = true
	client.AutoReconnectHook = func(error) bool {
		if client.AutoReconnectErrors > maxAutoReconnectErrors {
			client.AutoReconnectErrors = maxAutoReconnectErrors
		}
		return true
	}
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

func (w *waClient) IsConnected() bool {
	return w.client.IsConnected()
}

func (w *waClient) WaitForConnection(timeout time.Duration) bool {
	return w.client.WaitForConnection(timeout)
}

func (w *waClient) DeviceJID() *types.JID {
	return w.client.Store.ID
}

func (w *waClient) SendMessage(ctx context.Context, to types.JID, msg *waE2E.Message, id types.MessageID) (whatsmeow.SendResponse, error) {
	return w.client.SendMessage(ctx, to, msg, whatsmeow.SendRequestExtra{ID: id})
}

func (w *waClient) Upload(ctx context.Context, data []byte, mt whatsmeow.MediaType) (whatsmeow.UploadResponse, error) {
	return w.client.Upload(ctx, data, mt)
}

func (w *waClient) Download(ctx context.Context, msg whatsmeow.DownloadableMessage) ([]byte, error) {
	return w.client.Download(ctx, msg)
}

func (w *waClient) PNForLID(ctx context.Context, lid types.JID) (types.JID, bool, error) {
	pn, err := w.client.Store.LIDs.GetPNForLID(ctx, lid)
	if err != nil {
		return types.JID{}, false, err
	}
	return pn, !pn.IsEmpty(), nil
}

func (w *waClient) AddEventHandler(handler func(any)) uint32 {
	return w.client.AddEventHandler(handler)
}

func (w *waClient) Disconnect() {
	w.client.Disconnect()
}
