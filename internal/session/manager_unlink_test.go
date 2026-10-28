package session

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

type relinkClient struct {
	recordingClient
	mu  sync.Mutex
	jid *types.JID
}

func pairedRelinkClient() *relinkClient {
	jid := types.NewJID("15550000000", types.DefaultUserServer)
	return &relinkClient{jid: &jid}
}

func (c *relinkClient) DeviceJID() *types.JID {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.jid
}

func (c *relinkClient) IsConnected() bool {
	return c.DeviceJID() != nil
}

func (c *relinkClient) IsLoggedIn() bool {
	return c.DeviceJID() != nil
}

func (c *relinkClient) Logout(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.jid == nil {
		return whatsmeow.ErrNotLoggedIn
	}
	c.jid = nil
	return nil
}

func (c *relinkClient) QRChannel(context.Context) (<-chan whatsmeow.QRChannelItem, error) {
	jid := types.NewJID("15550000001", types.DefaultUserServer)
	c.mu.Lock()
	c.jid = &jid
	c.mu.Unlock()
	items := make(chan whatsmeow.QRChannelItem, 2)
	items <- whatsmeow.QRChannelItem{Event: "code", Code: "qr-relink", Timeout: time.Minute}
	items <- whatsmeow.QRChannelSuccess
	close(items)
	return items, nil
}

func TestRelinkForgetsTheUnlinkedDeviceOnceTheFreshPairingEnds(t *testing.T) {
	paired := pairedRelinkClient()
	fresh := &relinkClient{}
	clients := []WAClient{paired, fresh}
	m := NewManager(func(string, *types.JID) (WAClient, error) {
		next := clients[0]
		if len(clients) > 1 {
			clients = clients[1:]
		}
		return next, nil
	})

	updates, err := m.Pair(context.Background(), "channel-relink-forget", true, true)
	if err != nil {
		t.Fatalf("Pair failed: %v", err)
	}
	for range updates {
	}

	m.mu.Lock()
	marks := len(m.muted)
	current := m.sessions["channel-relink-forget"]
	m.mu.Unlock()
	if marks != 1 || current != WAClient(fresh) {
		t.Fatalf("the manager must keep only the fresh device of the channel, not the unlinked one, got %d marks and %v", marks, current)
	}
}
