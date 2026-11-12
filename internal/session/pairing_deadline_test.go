package session

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

// openQRClient yields one QR and then never closes its channel, like a QR nobody scans whose
// emitter stopped (expectedDisconnect): the pairing never ends by itself.
type openQRClient struct {
	relinkClient
}

func (c *openQRClient) QRChannel(context.Context) (<-chan whatsmeow.QRChannelItem, error) {
	items := make(chan whatsmeow.QRChannelItem, 1)
	items <- whatsmeow.QRChannelItem{Event: whatsmeow.QRChannelEventCode, Code: "qr-first", Timeout: time.Minute}
	return items, nil
}

// openQRManager's pairings are halted when the test ends: the pairing slot is global to the process.
func openQRManager(t *testing.T, options ...Option) *Manager {
	m := NewManager(func(string, *types.JID) (WAClient, error) { return &openQRClient{}, nil }, options...)
	t.Cleanup(func() {
		m.mu.Lock()
		live := make([]*pairing, 0, len(m.pairings))
		for _, p := range m.pairings {
			live = append(live, p)
		}
		m.mu.Unlock()
		for _, p := range live {
			p.halt()
			select {
			case <-p.done:
			case <-time.After(3 * time.Second): // a pairing faked by a test never closes done
			}
		}
	})
	return m
}

// TestUnscannedHistoryPairingFreesTheSlotForAnotherChannel: C1 + C3.
func TestUnscannedHistoryPairingFreesTheSlotForAnotherChannel(t *testing.T) {
	m := openQRManager(t, WithPairingMaxDuration(150*time.Millisecond), WithPairingSlotWait(3*time.Second))

	first, err := m.Pair(context.Background(), "card-6af6-a", true, false)
	if err != nil {
		t.Fatalf("first Pair: %v", err)
	}
	if update := <-first; update.QR != "qr-first" {
		t.Fatalf("first update = %+v, want the QR", update)
	}

	started := time.Now()
	second, err := m.Pair(context.Background(), "card-6af6-b", true, false)
	if err != nil {
		t.Fatalf("the second channel must get the slot once the first pairing hits its cap: %v", err)
	}
	if waited := time.Since(started); waited < 100*time.Millisecond {
		t.Fatalf("second channel got the slot after %s, before the first one ended", waited)
	}
	<-second

	var failure error
	for update := range first {
		failure = update.Err
	}
	if !errors.Is(failure, ErrPairingTimeout) {
		t.Fatalf("first pairing ended with %v, want ErrPairingTimeout", failure)
	}
}

// TestStalledUpdateReaderDoesNotHoldTheSlot: C2. Nobody reads the updates of the first pairing.
func TestStalledUpdateReaderDoesNotHoldTheSlot(t *testing.T) {
	m := openQRManager(t, WithPairingMaxDuration(150*time.Millisecond), WithPairingSlotWait(3*time.Second))

	if _, err := m.Pair(context.Background(), "card-6af6-stalled", true, false); err != nil {
		t.Fatalf("first Pair: %v", err)
	}
	// the reader never drains `updates`: the goroutine blocks sending the QR.
	if _, err := m.Pair(context.Background(), "card-6af6-next", true, false); err != nil {
		t.Fatalf("a stalled reader kept the pairing slot: %v", err)
	}
}

// TestSlotStaysBusyWhileTheFirstPairingIsWithinItsCap is the negative case of C1.
func TestSlotStaysBusyWhileTheFirstPairingIsWithinItsCap(t *testing.T) {
	m := openQRManager(t, WithPairingMaxDuration(time.Minute), WithPairingSlotWait(100*time.Millisecond))

	first, err := m.Pair(context.Background(), "card-6af6-live", true, false)
	if err != nil {
		t.Fatalf("first Pair: %v", err)
	}
	<-first
	if _, err := m.Pair(context.Background(), "card-6af6-wait", true, false); err == nil {
		t.Fatal("the second channel must be refused while the first pairing is within its cap")
	}
}

// TestSupersededPairingThatNeverEndsDoesNotHangTheRequest: C4.
func TestSupersededPairingThatNeverEndsDoesNotHangTheRequest(t *testing.T) {
	m := openQRManager(t, WithSupersededWait(100*time.Millisecond))
	m.pairings["card-6af6-old"] = newPairing(false, "") // its goroutine is gone: done never closes

	started := time.Now()
	_, err := m.Pair(context.Background(), "card-6af6-old", true, false)
	if err == nil || !strings.Contains(err.Error(), "did not end") {
		t.Fatalf("err = %v, want the superseded-wait error", err)
	}
	if waited := time.Since(started); waited > 2*time.Second {
		t.Fatalf("waited %s for the superseded pairing", waited)
	}
}
