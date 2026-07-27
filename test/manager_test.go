package test

import (
	"context"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/w3nder/whatsmeow-gateway/internal/session"
)

func TestManagerPairEmitsQRThenSuccess(t *testing.T) {
	fake := newFakeWAClient()
	fake.qrItems = []whatsmeow.QRChannelItem{
		{Event: "code", Code: "qr-code-1"},
		whatsmeow.QRChannelSuccess,
	}

	mgr := session.NewManager(func(channelID string) (session.WAClient, error) {
		return fake, nil
	})

	updates, err := mgr.Pair(context.Background(), "channel-1")
	if err != nil {
		t.Fatalf("Pair failed: %v", err)
	}

	var got []session.PairUpdate
	for u := range updates {
		got = append(got, u)
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 updates, got %d: %+v", len(got), got)
	}
	if got[0].QR != "qr-code-1" {
		t.Fatalf("expected first update QR=qr-code-1, got %+v", got[0])
	}
	if !got[1].Connected {
		t.Fatalf("expected second update Connected=true, got %+v", got[1])
	}
	if fake.connectCalls != 1 {
		t.Fatalf("expected Connect called once, got %d", fake.connectCalls)
	}
}

func TestManagerDispatchesMessageEventWithChannelID(t *testing.T) {
	fake := newFakeWAClient()
	mgr := session.NewManager(func(channelID string) (session.WAClient, error) {
		return fake, nil
	})

	type received struct {
		channelID string
		evt       any
	}
	recv := make(chan received, 1)
	mgr.OnEvent(func(channelID string, evt any) {
		recv <- received{channelID: channelID, evt: evt}
	})

	if err := mgr.EnsureConnected("channel-2"); err != nil {
		t.Fatalf("EnsureConnected failed: %v", err)
	}

	msgEvt := &events.Message{Info: types.MessageInfo{}}
	fake.emit(msgEvt)

	select {
	case r := <-recv:
		if r.channelID != "channel-2" {
			t.Fatalf("expected channelID channel-2, got %s", r.channelID)
		}
		if r.evt != any(msgEvt) {
			t.Fatalf("expected the injected message event, got %+v", r.evt)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for dispatched event")
	}
}

func TestManagerDropsSessionOnLoggedOut(t *testing.T) {
	first := newFakeWAClient()
	callCount := 0
	mgr := session.NewManager(func(channelID string) (session.WAClient, error) {
		callCount++
		if callCount == 1 {
			return first, nil
		}
		return newFakeWAClient(), nil
	})

	if err := mgr.EnsureConnected("channel-3"); err != nil {
		t.Fatalf("EnsureConnected failed: %v", err)
	}
	if callCount != 1 {
		t.Fatalf("expected factory called once, got %d", callCount)
	}

	first.emit(&events.LoggedOut{})

	if err := mgr.EnsureConnected("channel-3"); err != nil {
		t.Fatalf("EnsureConnected after logout failed: %v", err)
	}
	if callCount != 2 {
		t.Fatalf("expected factory called again after logout, got %d", callCount)
	}
}

func TestManagerSendReturnsIDAndTimestamp(t *testing.T) {
	fake := newFakeWAClient()
	ts := time.Now().Truncate(time.Second)
	fake.sendResp = whatsmeow.SendResponse{ID: "msg-1", Timestamp: ts}

	mgr := session.NewManager(func(channelID string) (session.WAClient, error) {
		return fake, nil
	})

	to := types.NewJID("15551234567", types.DefaultUserServer)
	id, gotTS, err := mgr.Send(context.Background(), "channel-4", to, nil)
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	if id != "msg-1" {
		t.Fatalf("expected id msg-1, got %s", id)
	}
	if !gotTS.Equal(ts) {
		t.Fatalf("expected timestamp %v, got %v", ts, gotTS)
	}
}

func TestManagerPairGoroutineStopsOnContextCancel(t *testing.T) {
	fake := newFakeWAClient()
	fake.qrItems = []whatsmeow.QRChannelItem{
		{Event: "code", Code: "qr-code-1"},
		{Event: "code", Code: "qr-code-2"},
		whatsmeow.QRChannelSuccess,
	}

	mgr := session.NewManager(func(channelID string) (session.WAClient, error) {
		return fake, nil
	})

	ctx, cancel := context.WithCancel(context.Background())

	updates, err := mgr.Pair(ctx, "channel-5")
	if err != nil {
		t.Fatalf("Pair failed: %v", err)
	}

	first := <-updates
	if first.QR != "qr-code-1" {
		t.Fatalf("expected first update QR=qr-code-1, got %+v", first)
	}

	cancel()

	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-updates:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("Pair goroutine did not stop after context cancellation (leak)")
		}
	}
}
