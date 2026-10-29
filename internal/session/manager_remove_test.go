package session

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

type removableClient struct {
	recordingClient
	logoutErr   error
	logouts     atomic.Int32
	disconnects atomic.Int32
}

func (c *removableClient) Logout(context.Context) error {
	c.logouts.Add(1)
	return c.logoutErr
}

func (c *removableClient) Disconnect() {
	c.disconnects.Add(1)
}

func managerWithSession(t *testing.T, channelID string, client WAClient) *Manager {
	t.Helper()
	m := NewManager(func(string, *types.JID) (WAClient, error) { return client, nil })
	if _, err := m.client(channelID); err != nil {
		t.Fatalf("client: %v", err)
	}
	return m
}

func TestRemoveLogsOutAndForgetsTheSessionOfTheChannel(t *testing.T) {
	client := &removableClient{}
	m := managerWithSession(t, "channel-removed", client)

	if err := m.Remove(context.Background(), "channel-removed"); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	m.mu.Lock()
	_, kept := m.sessions["channel-removed"]
	_, marked := m.muted["channel-removed"]
	m.mu.Unlock()
	if kept || marked {
		t.Fatal("the removed channel must leave the manager with no session and no mute mark")
	}
	if client.logouts.Load() != 1 || client.disconnects.Load() != 1 {
		t.Fatalf("the device must log out once and disconnect once, got %d logouts and %d disconnects", client.logouts.Load(), client.disconnects.Load())
	}
}

func TestRemoveDropsTheSessionEvenWhenTheLogoutFails(t *testing.T) {
	client := &removableClient{logoutErr: errors.New("phone unreachable")}
	m := managerWithSession(t, "channel-offline", client)

	if err := m.Remove(context.Background(), "channel-offline"); err == nil {
		t.Fatal("a failed logout must be reported to the caller")
	}

	m.mu.Lock()
	_, kept := m.sessions["channel-offline"]
	m.mu.Unlock()
	if kept || client.disconnects.Load() != 1 {
		t.Fatal("the socket must be closed and the session forgotten even when the phone cannot be reached")
	}
}

func TestRemoveTreatsAnAlreadyLoggedOutDeviceAsDone(t *testing.T) {
	client := &removableClient{logoutErr: whatsmeow.ErrNotLoggedIn}
	m := managerWithSession(t, "channel-gone", client)

	if err := m.Remove(context.Background(), "channel-gone"); err != nil {
		t.Fatalf("a device that already logged out is not a failure, got %v", err)
	}
}

func TestRemoveOfAChannelWithoutSessionDoesNothing(t *testing.T) {
	m := NewManager(func(string, *types.JID) (WAClient, error) {
		t.Fatal("removing must never create a client")
		return nil, nil
	})

	if err := m.Remove(context.Background(), "channel-unknown"); err != nil {
		t.Fatalf("Remove of an unknown channel must be a no-op, got %v", err)
	}
}
