package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

func TestPairingEndErrorKeepsTheQRTimeoutApartFromTheCodeExpiry(t *testing.T) {
	timeout := whatsmeow.QRChannelTimeout

	if err := pairingEndError(&pairing{}, timeout); !errors.Is(err, ErrPairingTimeout) {
		t.Errorf("qr timeout = %v, want ErrPairingTimeout", err)
	}
	if err := pairingEndError(&pairing{phone: "5511987654321"}, timeout); !errors.Is(err, ErrPairingCodeExpired) {
		t.Errorf("code timeout = %v, want ErrPairingCodeExpired", err)
	}
	if err := pairingEndError(&pairing{}, whatsmeow.QRChannelErrUnexpectedEvent); errors.Is(err, ErrPairingTimeout) {
		t.Errorf("unexpected event = %v, must not read as a timeout", err)
	}
}

type unfinishedQRClient struct {
	relinkClient
}

func (c *unfinishedQRClient) QRChannel(context.Context) (<-chan whatsmeow.QRChannelItem, error) {
	items := make(chan whatsmeow.QRChannelItem, 2)
	items <- whatsmeow.QRChannelItem{Event: whatsmeow.QRChannelEventCode, Code: "qr-first", Timeout: time.Minute}
	items <- whatsmeow.QRChannelItem{Event: whatsmeow.QRChannelEventPasskeyRequest}
	return items, nil
}

func TestPairingEventThatFailsWithoutClosingTheQRChannelFreesTheNextPairing(t *testing.T) {
	m := NewManager(func(string, *types.JID) (WAClient, error) {
		return &unfinishedQRClient{}, nil
	})

	updates, err := m.Pair(context.Background(), "channel-passkey", false, false)
	if err != nil {
		t.Fatalf("Pair failed: %v", err)
	}

	var failure error
	deadline := time.After(5 * time.Second)
	for finished := false; !finished; {
		select {
		case update, open := <-updates:
			if !open {
				finished = true
				break
			}
			if update.Err != nil {
				failure = update.Err
			}
		case <-deadline:
			t.Fatal("the pairing kept running after a failure that does not close the QR channel")
		}
	}
	if failure == nil {
		t.Fatal("the failure must reach the caller")
	}

	m.mu.Lock()
	_, live := m.pairings["channel-passkey"]
	m.mu.Unlock()
	if live {
		t.Fatal("the failed pairing must not stay registered, or the next request joins it without a QR")
	}
}
