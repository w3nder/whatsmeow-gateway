package session

import (
	"context"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

type silentlyStoppedQRClient struct {
	recordingClient
}

func (c *silentlyStoppedQRClient) QRChannel(context.Context) (<-chan whatsmeow.QRChannelItem, error) {
	items := make(chan whatsmeow.QRChannelItem, 1)
	items <- whatsmeow.QRChannelItem{Event: whatsmeow.QRChannelEventCode, Code: "qr-open", Timeout: time.Minute}
	return items, nil
}

func TestRemovingAChannelWhileItsQRIsOpenEndsThePairingSoTheNextOneShowsAFreshQR(t *testing.T) {
	m := NewManager(func(string, *types.JID) (WAClient, error) {
		return &silentlyStoppedQRClient{}, nil
	})

	updates, err := m.Pair(context.Background(), "channel-removed-mid-qr", false, false)
	if err != nil {
		t.Fatalf("Pair failed: %v", err)
	}
	if update := <-updates; update.QR != "qr-open" {
		t.Fatalf("first update = %+v, want the open QR", update)
	}

	if err := m.Remove(context.Background(), "channel-removed-mid-qr"); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	select {
	case _, open := <-updates:
		if open {
			t.Fatal("the removed channel must not emit pairing updates")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the pairing of the removed channel kept running after its socket was closed")
	}

	m.mu.Lock()
	_, live := m.pairings["channel-removed-mid-qr"]
	m.mu.Unlock()
	if live {
		t.Fatal("the pairing of the removed channel must not stay registered")
	}

	again, err := m.Pair(context.Background(), "channel-removed-mid-qr", true, false)
	if err != nil {
		t.Fatalf("Pair after the removal failed: %v", err)
	}
	if update := <-again; update.QR != "qr-open" {
		t.Fatalf("the pairing after the removal must show its own QR, got %+v", update)
	}
	if err := m.Remove(context.Background(), "channel-removed-mid-qr"); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}
	for range again {
	}
}
