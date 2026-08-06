package session

import (
	"context"
	"log/slog"
	"time"

	"github.com/purpshell/meowcaller"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"

	"github.com/w3nder/whatsmeow-gateway/internal/call"
	"github.com/w3nder/whatsmeow-gateway/internal/logging"
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
	BuildEdit(chat types.JID, id types.MessageID, newContent *waE2E.Message) *waE2E.Message
	BuildRevoke(chat, sender types.JID, id types.MessageID) *waE2E.Message
	BuildReaction(chat, sender types.JID, id types.MessageID, reaction string) *waE2E.Message
	Upload(ctx context.Context, data []byte, mt whatsmeow.MediaType) (whatsmeow.UploadResponse, error)
	Download(ctx context.Context, msg whatsmeow.DownloadableMessage) ([]byte, error)
	PNForLID(ctx context.Context, lid types.JID) (types.JID, bool, error)
	GetProfilePictureInfo(ctx context.Context, jid types.JID, params *whatsmeow.GetProfilePictureParams) (*types.ProfilePictureInfo, error)
	GetGroupInfo(ctx context.Context, jid types.JID) (*types.GroupInfo, error)
	AddEventHandler(handler func(any)) uint32
	Calls() call.Caller
	Disconnect()
}

type waClient struct {
	client *whatsmeow.Client
	caller call.Caller
}

func NewWAClient(channelID string, device *store.Device, log waLog.Logger, slogger *slog.Logger) WAClient {
	client := whatsmeow.NewClient(device, log)
	ConfigureAutoReconnect(client)

	// The calling client installs the low-level <call>/<ack> interception, so it
	// has to exist before the receive loop starts -- that is, before Connect.
	//
	// meowcaller defaults to a no-op logger, which would leave it (including its own
	// startup failures) completely silent; bridge its zerolog output into the
	// gateway's slog so a dead calling stack is visible instead of indistinguishable
	// from no call ever arriving.
	caller := newCallerAdapter(meowcaller.NewClient(client, meowcaller.WithLogger(logging.NewCallLogger(slogger, channelID))))

	return &waClient{client: client, caller: caller}
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

func (w *waClient) BuildEdit(chat types.JID, id types.MessageID, newContent *waE2E.Message) *waE2E.Message {
	return w.client.BuildEdit(chat, id, newContent)
}

func (w *waClient) BuildRevoke(chat, sender types.JID, id types.MessageID) *waE2E.Message {
	return w.client.BuildRevoke(chat, sender, id)
}

func (w *waClient) BuildReaction(chat, sender types.JID, id types.MessageID, reaction string) *waE2E.Message {
	return w.client.BuildReaction(chat, sender, id, reaction)
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

func (w *waClient) GetProfilePictureInfo(ctx context.Context, jid types.JID, params *whatsmeow.GetProfilePictureParams) (*types.ProfilePictureInfo, error) {
	return w.client.GetProfilePictureInfo(ctx, jid, params)
}

func (w *waClient) GetGroupInfo(ctx context.Context, jid types.JID) (*types.GroupInfo, error) {
	return w.client.GetGroupInfo(ctx, jid)
}

func (w *waClient) AddEventHandler(handler func(any)) uint32 {
	return w.client.AddEventHandler(handler)
}

func (w *waClient) Calls() call.Caller {
	return w.caller
}

func (w *waClient) Disconnect() {
	w.client.Disconnect()
}
