package test

import (
	"context"
	"errors"
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

	mgr := session.NewManager(func(channelID string, jid *types.JID) (session.WAClient, error) {
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
	mgr := session.NewManager(func(channelID string, jid *types.JID) (session.WAClient, error) {
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

	jid := types.NewJID("15551234567", types.DefaultUserServer)
	if err := mgr.Resume(context.Background(), "channel-2", jid); err != nil {
		t.Fatalf("Resume failed: %v", err)
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
	mgr := session.NewManager(func(channelID string, jid *types.JID) (session.WAClient, error) {
		callCount++
		if callCount == 1 {
			return first, nil
		}
		return newFakeWAClient(), nil
	})

	jid := types.NewJID("15551234567", types.DefaultUserServer)
	if err := mgr.Resume(context.Background(), "channel-3", jid); err != nil {
		t.Fatalf("Resume failed: %v", err)
	}
	if callCount != 1 {
		t.Fatalf("expected factory called once, got %d", callCount)
	}

	first.emit(&events.LoggedOut{})

	deadline := time.After(2 * time.Second)
	for {
		err := mgr.EnsureConnected("channel-3")
		if errors.Is(err, session.ErrNoSession) {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("expected the logged-out session to be dropped so it is re-paired instead of silently reconnected, got %v", err)
		case <-time.After(10 * time.Millisecond):
		}
	}
	if callCount != 1 {
		t.Fatalf("expected no new device to be built after logout, factory called %d times", callCount)
	}
	if first.disconnectCount() == 0 {
		t.Fatal("expected the dropped session's socket to be closed on logout")
	}
}

func TestManagerEnsureConnectedReconnectsAfterSocketDrop(t *testing.T) {
	fake := newFakeWAClient()
	mgr := session.NewManager(func(channelID string, jid *types.JID) (session.WAClient, error) {
		return fake, nil
	})

	jid := types.NewJID("15551234567", types.DefaultUserServer)
	if err := mgr.Resume(context.Background(), "channel-drop-1", jid); err != nil {
		t.Fatalf("Resume failed: %v", err)
	}
	if fake.connectCallCount() != 1 {
		t.Fatalf("expected Connect once during Resume, got %d", fake.connectCallCount())
	}

	fake.dropSocket()

	if err := mgr.EnsureConnected("channel-drop-1"); err != nil {
		t.Fatalf("EnsureConnected after socket drop failed: %v", err)
	}
	if fake.connectCallCount() != 2 {
		t.Fatalf("expected EnsureConnected to reconnect the dead socket, Connect called %d times", fake.connectCallCount())
	}
	if fake.qrChannelCallCount() != 0 {
		t.Fatalf("expected the reconnect to reuse the paired device with no QR flow, got %d QRChannel calls", fake.qrChannelCallCount())
	}
}

func TestManagerEnsureConnectedToleratesConcurrentAutoReconnect(t *testing.T) {
	fake := newFakeWAClient()
	mgr := session.NewManager(func(channelID string, jid *types.JID) (session.WAClient, error) {
		return fake, nil
	})

	jid := types.NewJID("15551234567", types.DefaultUserServer)
	if err := mgr.Resume(context.Background(), "channel-race-1", jid); err != nil {
		t.Fatalf("Resume failed: %v", err)
	}

	fake.dropSocket()
	fake.connectErr = whatsmeow.ErrAlreadyConnected
	fake.connected = true

	if err := mgr.EnsureConnected("channel-race-1"); err != nil {
		t.Fatalf("expected ErrAlreadyConnected to be treated as connected, got %v", err)
	}
}

func TestManagerEnsureConnectedNeverBuildsUnpairedDevice(t *testing.T) {
	factoryCalls := 0
	mgr := session.NewManager(func(channelID string, jid *types.JID) (session.WAClient, error) {
		factoryCalls++
		return newFakeWAClient(), nil
	})

	err := mgr.EnsureConnected("channel-never-paired")

	if !errors.Is(err, session.ErrNoSession) {
		t.Fatalf("expected ErrNoSession for a channel with no live session, got %v", err)
	}
	if factoryCalls != 0 {
		t.Fatalf("expected no device to be built for an unknown channel (a fresh device would require re-pairing), factory called %d times", factoryCalls)
	}
}

func TestManagerResumeRegistersEventHandlerBeforeConnecting(t *testing.T) {
	fake := newFakeWAClient()
	mgr := session.NewManager(func(channelID string, jid *types.JID) (session.WAClient, error) {
		return fake, nil
	})

	jid := types.NewJID("15551234567", types.DefaultUserServer)
	if err := mgr.Resume(context.Background(), "channel-handler-1", jid); err != nil {
		t.Fatalf("Resume failed: %v", err)
	}

	if fake.handlerCountAtConnect() == 0 {
		t.Fatal("expected the event handler to be registered before Connect, otherwise connection events emitted during the handshake are lost")
	}
}

func TestManagerSendReturnsIDAndTimestamp(t *testing.T) {
	fake := newFakeWAClient()
	ts := time.Now().Truncate(time.Second)
	fake.sendResp = whatsmeow.SendResponse{ID: "msg-1", Timestamp: ts}

	mgr := session.NewManager(func(channelID string, jid *types.JID) (session.WAClient, error) {
		return fake, nil
	})

	if err := mgr.Resume(context.Background(), "channel-4", types.NewJID("15550001111", types.DefaultUserServer)); err != nil {
		t.Fatalf("Resume failed: %v", err)
	}

	to := types.NewJID("15551234567", types.DefaultUserServer)
	id, gotTS, err := mgr.Send(context.Background(), "channel-4", to, nil, "deterministic-id-1", nil)
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	if id != "msg-1" {
		t.Fatalf("expected id msg-1, got %s", id)
	}
	if !gotTS.Equal(ts) {
		t.Fatalf("expected timestamp %v, got %v", ts, gotTS)
	}
	if fake.lastID != "deterministic-id-1" {
		t.Fatalf("expected Send to pass the explicit id through to WAClient.SendMessage, got %q", fake.lastID)
	}
}

func TestManagerResumeConnectsWithoutQR(t *testing.T) {
	fake := newFakeWAClient()

	var factoryCalledWithJID *types.JID
	mgr := session.NewManager(func(channelID string, jid *types.JID) (session.WAClient, error) {
		factoryCalledWithJID = jid
		return fake, nil
	})

	jid := types.NewJID("15551234567", types.DefaultUserServer)

	if err := mgr.Resume(context.Background(), "channel-resume-1", jid); err != nil {
		t.Fatalf("Resume failed: %v", err)
	}

	if factoryCalledWithJID == nil || *factoryCalledWithJID != jid {
		t.Fatalf("expected the factory to be called with the stored jid %v, got %v", jid, factoryCalledWithJID)
	}
	if fake.connectCallCount() != 1 {
		t.Fatalf("expected Connect called once, got %d", fake.connectCallCount())
	}
	if fake.qrChannelCallCount() != 0 {
		t.Fatalf("expected no QR flow during Resume, got %d QRChannel calls", fake.qrChannelCallCount())
	}
}

func TestManagerResumeRegistersSessionForSubsequentUse(t *testing.T) {
	fake := newFakeWAClient()

	factoryCalls := 0
	mgr := session.NewManager(func(channelID string, jid *types.JID) (session.WAClient, error) {
		factoryCalls++
		return fake, nil
	})

	jid := types.NewJID("15551234567", types.DefaultUserServer)
	if err := mgr.Resume(context.Background(), "channel-resume-2", jid); err != nil {
		t.Fatalf("Resume failed: %v", err)
	}
	if factoryCalls != 1 {
		t.Fatalf("expected factory called once during Resume, got %d", factoryCalls)
	}

	if err := mgr.EnsureConnected("channel-resume-2"); err != nil {
		t.Fatalf("EnsureConnected after Resume failed: %v", err)
	}
	if factoryCalls != 1 {
		t.Fatalf("expected EnsureConnected to reuse the resumed session, factory called %d times", factoryCalls)
	}
}

func TestManagerPairGoroutineStopsOnContextCancel(t *testing.T) {
	fake := newFakeWAClient()
	fake.qrItems = []whatsmeow.QRChannelItem{
		{Event: "code", Code: "qr-code-1"},
		{Event: "code", Code: "qr-code-2"},
		whatsmeow.QRChannelSuccess,
	}

	mgr := session.NewManager(func(channelID string, jid *types.JID) (session.WAClient, error) {
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
