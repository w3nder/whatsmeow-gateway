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

func TestRemovingAChannelWhileItsQRIsOpenEndsThePairingAndFreesTheHistorySlot(t *testing.T) {
	m := NewManager(func(string, *types.JID) (WAClient, error) {
		return &silentlyStoppedQRClient{}, nil
	}, WithPairingSlotWait(2*time.Second))

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

	history, err := m.Pair(context.Background(), "channel-importing-history", true, false)
	if err != nil {
		t.Fatalf("a history pairing after the removal must get the slot, got %v", err)
	}
	if update := <-history; update.QR != "qr-open" {
		t.Fatalf("history pairing update = %+v, want its QR", update)
	}
	if err := m.Remove(context.Background(), "channel-importing-history"); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}
	for range history {
	}
}
