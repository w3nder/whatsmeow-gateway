package test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	wastore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/w3nder/whatsmeow-gateway/internal/deviceprops"
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

	updates, err := mgr.Pair(context.Background(), "channel-1", false)
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
	if err := mgr.Resume(context.Background(), "channel-2", jid, false); err != nil {
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
	if err := mgr.Resume(context.Background(), "channel-3", jid, false); err != nil {
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
	if err := mgr.Resume(context.Background(), "channel-drop-1", jid, false); err != nil {
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
	if err := mgr.Resume(context.Background(), "channel-race-1", jid, false); err != nil {
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
	if err := mgr.Resume(context.Background(), "channel-handler-1", jid, false); err != nil {
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

	if err := mgr.Resume(context.Background(), "channel-4", types.NewJID("15550001111", types.DefaultUserServer), false); err != nil {
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

	if err := mgr.Resume(context.Background(), "channel-resume-1", jid, false); err != nil {
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
	if err := mgr.Resume(context.Background(), "channel-resume-2", jid, false); err != nil {
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

	updates, err := mgr.Pair(ctx, "channel-5", false)
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

func managerFor(clients map[string]*fakeWAClient) *session.Manager {
	return session.NewManager(func(channelID string, _ *types.JID) (session.WAClient, error) {
		return clients[channelID], nil
	})
}

func drainPairing(t *testing.T, mgr *session.Manager, channelID string, importHistory bool) {
	t.Helper()
	updates, err := mgr.Pair(context.Background(), channelID, importHistory)
	if err != nil {
		t.Fatalf("Pair failed: %v", err)
	}
	for range updates {
	}
}

func TestManagerPairWithHistoryConnectsWithTheHistoryPropsAndRestoresThemOnSuccess(t *testing.T) {
	fake := newFakeWAClient()
	fake.qrItems = []whatsmeow.QRChannelItem{{Event: "code", Code: "qr-history-ok"}, whatsmeow.QRChannelSuccess}
	defaults := wastore.DeviceProps

	drainPairing(t, managerFor(map[string]*fakeWAClient{"channel-history-ok": fake}), "channel-history-ok", true)

	props := fake.connectProps()
	if props == defaults || props.GetPlatformType() != deviceprops.HistoryPlatform || props.GetHistorySyncConfig().GetFullSyncDaysLimit() != deviceprops.WindowDays {
		t.Fatalf("the pairing connect must carry the history props, got %v", props)
	}
	if wastore.DeviceProps != defaults {
		t.Fatal("the props must be restored once the pairing succeeds")
	}
}

func TestManagerPairWithHistoryRestoresThePropsWhenTheQRTimesOut(t *testing.T) {
	fake := newFakeWAClient()
	fake.qrItems = []whatsmeow.QRChannelItem{{Event: "code", Code: "qr-history-timeout"}, whatsmeow.QRChannelTimeout}
	defaults := wastore.DeviceProps

	drainPairing(t, managerFor(map[string]*fakeWAClient{"channel-history-timeout": fake}), "channel-history-timeout", true)

	if wastore.DeviceProps != defaults {
		t.Fatal("the props must be restored once the QR times out")
	}
}

func TestManagerPairWithHistoryRestoresThePropsWhenThePairingFails(t *testing.T) {
	fake := newFakeWAClient()
	fake.qrItems = []whatsmeow.QRChannelItem{{Event: "code", Code: "qr-history-err"}, {Event: "err-client-outdated"}}
	defaults := wastore.DeviceProps

	drainPairing(t, managerFor(map[string]*fakeWAClient{"channel-history-err": fake}), "channel-history-err", true)

	if wastore.DeviceProps != defaults {
		t.Fatal("the props must be restored once the pairing fails")
	}
}

func TestManagerPairWithHistoryRestoresThePropsWhenTheConnectFails(t *testing.T) {
	fake := newFakeWAClient()
	fake.connectErr = errors.New("dial refused")
	defaults := wastore.DeviceProps

	if _, err := managerFor(map[string]*fakeWAClient{"channel-history-dial": fake}).Pair(context.Background(), "channel-history-dial", true); err == nil {
		t.Fatal("a failed connect must fail the pairing")
	}
	if wastore.DeviceProps != defaults {
		t.Fatal("the props must be restored when the pairing connect fails")
	}
}

func TestManagerPairWithoutHistoryConnectsWithTodaysProps(t *testing.T) {
	fake := newFakeWAClient()
	fake.qrItems = []whatsmeow.QRChannelItem{{Event: "code", Code: "qr-default-props"}, whatsmeow.QRChannelSuccess}
	defaults := wastore.DeviceProps

	drainPairing(t, managerFor(map[string]*fakeWAClient{"channel-default-props": fake}), "channel-default-props", false)

	if fake.connectProps() != defaults {
		t.Fatal("an opted-out pairing must connect with today's props")
	}
}

func TestManagerPairOfAnotherChannelWaitsForAHistoryPairingToFinish(t *testing.T) {
	importing := newFakeWAClient()
	importing.qrFeed = make(chan whatsmeow.QRChannelItem)
	waiting := newFakeWAClient()
	waiting.qrItems = []whatsmeow.QRChannelItem{{Event: "code", Code: "qr-waiting"}, whatsmeow.QRChannelSuccess}
	defaults := wastore.DeviceProps
	mgr := managerFor(map[string]*fakeWAClient{"channel-importing": importing, "channel-waiting": waiting})

	first, err := mgr.Pair(context.Background(), "channel-importing", true)
	if err != nil {
		t.Fatalf("Pair(importing) failed: %v", err)
	}
	go func() {
		for range first {
		}
	}()
	secondDone := make(chan error, 1)
	go func() {
		updates, err := mgr.Pair(context.Background(), "channel-waiting", false)
		if err == nil {
			for range updates {
			}
		}
		secondDone <- err
	}()

	time.Sleep(200 * time.Millisecond)
	if waiting.connectCallCount() != 0 {
		t.Fatal("no other pairing may connect while a history pairing is still showing its QR")
	}

	close(importing.qrFeed)
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("Pair(waiting) failed: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the waiting pairing must run once the history pairing ends")
	}
	if waiting.connectProps() != defaults {
		t.Fatal("the pairing after a history pairing must connect with today's props")
	}
}

func TestManagerPairWithHistoryTakesOverTheHistoryBeforeConnecting(t *testing.T) {
	fake := newFakeWAClient()
	fake.qrItems = []whatsmeow.QRChannelItem{{Event: "code", Code: "qr-takeover"}, whatsmeow.QRChannelSuccess}

	drainPairing(t, managerFor(map[string]*fakeWAClient{"channel-takeover": fake}), "channel-takeover", true)

	if !fake.tookOverHistoryBeforeConnecting() {
		t.Fatal("an importing pairing must switch its client to manual history download before connecting")
	}
}

func TestManagerPairWithoutHistoryLeavesTheHistoryToWhatsmeow(t *testing.T) {
	fake := newFakeWAClient()
	fake.qrItems = []whatsmeow.QRChannelItem{{Event: "code", Code: "qr-automatic"}, whatsmeow.QRChannelSuccess}

	drainPairing(t, managerFor(map[string]*fakeWAClient{"channel-automatic": fake}), "channel-automatic", false)

	if fake.TakesOverHistory() {
		t.Fatal("a pairing without import must keep whatsmeow's automatic history download")
	}
}

func TestManagerResumeOfAnImportingChannelTakesOverTheHistoryBeforeConnecting(t *testing.T) {
	fake := newFakeWAClient()
	mgr := managerFor(map[string]*fakeWAClient{"channel-resume-importing": fake})

	if err := mgr.Resume(context.Background(), "channel-resume-importing", types.NewJID("15550002222", types.DefaultUserServer), true); err != nil {
		t.Fatalf("Resume failed: %v", err)
	}

	if !fake.tookOverHistoryBeforeConnecting() {
		t.Fatal("a channel resumed in the middle of an import must switch to manual history download before connecting")
	}
}

func TestManagerResumeOfAnIdleChannelLeavesTheHistoryToWhatsmeow(t *testing.T) {
	fake := newFakeWAClient()
	mgr := managerFor(map[string]*fakeWAClient{"channel-resume-idle": fake})

	if err := mgr.Resume(context.Background(), "channel-resume-idle", types.NewJID("15550003333", types.DefaultUserServer), false); err != nil {
		t.Fatalf("Resume failed: %v", err)
	}

	if fake.TakesOverHistory() {
		t.Fatal("a channel without an import must keep whatsmeow's automatic history download")
	}
}

func managerOver(clients ...*fakeWAClient) *session.Manager {
	var mu sync.Mutex
	return session.NewManager(func(string, *types.JID) (session.WAClient, error) {
		mu.Lock()
		defer mu.Unlock()
		next := clients[0]
		if len(clients) > 1 {
			clients = clients[1:]
		}
		return next, nil
	})
}

func pairInBackground(mgr *session.Manager, channelID string, importHistory bool) (<-chan session.PairUpdate, <-chan error) {
	firstUpdates := make(chan (<-chan session.PairUpdate), 1)
	failed := make(chan error, 1)
	go func() {
		updates, err := mgr.Pair(context.Background(), channelID, importHistory)
		if err != nil {
			failed <- err
			return
		}
		firstUpdates <- updates
	}()
	select {
	case updates := <-firstUpdates:
		return updates, failed
	case <-time.After(10 * time.Second):
		return nil, failed
	}
}

func waitClosed(t *testing.T, updates <-chan session.PairUpdate, what string) []session.PairUpdate {
	t.Helper()
	var got []session.PairUpdate
	deadline := time.After(10 * time.Second)
	for {
		select {
		case update, ok := <-updates:
			if !ok {
				return got
			}
			got = append(got, update)
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

func TestManagerPairWithHistoryRestartsAPairingInFlightWithoutHistory(t *testing.T) {
	plain := newFakeWAClient()
	plain.qrFeed = make(chan whatsmeow.QRChannelItem)
	importing := newFakeWAClient()
	importing.qrItems = []whatsmeow.QRChannelItem{{Event: "code", Code: "qr-restarted"}, whatsmeow.QRChannelSuccess}
	defaults := wastore.DeviceProps
	mgr := managerOver(plain, importing)

	first, err := mgr.Pair(context.Background(), "channel-restart", false)
	if err != nil {
		t.Fatalf("Pair(without history) failed: %v", err)
	}
	select {
	case plain.qrFeed <- whatsmeow.QRChannelItem{Event: "code", Code: "qr-plain", Timeout: time.Minute}:
	case <-time.After(10 * time.Second):
		t.Fatal("the pairing without history never read its QR")
	}
	if update := <-first; update.QR != "qr-plain" {
		t.Fatalf("the pairing without history must show its QR first, got %+v", update)
	}

	second, failed := pairInBackground(mgr, "channel-restart", true)
	if second == nil {
		t.Fatalf("the pairing with history must restart the one in flight, got %v", <-failed)
	}

	waitClosed(t, first, "the pairing without history to end")
	if plain.disconnectCount() == 0 {
		t.Fatal("the client of the pairing without history must be dropped before the restart")
	}
	got := waitClosed(t, second, "the restarted pairing to finish")
	if len(got) != 2 || got[0].QR != "qr-restarted" || !got[1].Connected {
		t.Fatalf("the restarted pairing must show its own QR and connect, got %+v", got)
	}
	if !importing.tookOverHistoryBeforeConnecting() {
		t.Fatal("the restarted pairing must download the history itself")
	}
	if props := importing.connectProps(); props == defaults || props.GetPlatformType() != deviceprops.HistoryPlatform {
		t.Fatalf("the restarted pairing must connect with the history props, got %v", props)
	}
	if wastore.DeviceProps != defaults {
		t.Fatal("the props must be restored after the restarted pairing")
	}
}

func TestManagerPairWithHistoryJoinsAPairingInFlightThatAlreadyImports(t *testing.T) {
	importing := newFakeWAClient()
	importing.qrFeed = make(chan whatsmeow.QRChannelItem)
	unused := newFakeWAClient()
	mgr := managerOver(importing, unused)

	first, err := mgr.Pair(context.Background(), "channel-join-history", true)
	if err != nil {
		t.Fatalf("Pair(with history) failed: %v", err)
	}
	select {
	case importing.qrFeed <- whatsmeow.QRChannelItem{Event: "code", Code: "qr-history", Timeout: time.Minute}:
	case <-time.After(10 * time.Second):
		t.Fatal("the pairing with history never read its QR")
	}
	<-first

	second, err := mgr.Pair(context.Background(), "channel-join-history", true)
	if err != nil {
		t.Fatalf("the second pairing with history must join, got %v", err)
	}
	if got := waitClosed(t, second, "the joined replay"); len(got) != 1 || got[0].QR != "qr-history" {
		t.Fatalf("the second request must receive the QR in flight, got %+v", got)
	}
	if importing.disconnectCount() != 0 || importing.connectCallCount() != 1 || unused.connectCallCount() != 0 {
		t.Fatal("a pairing that already imports must never be restarted")
	}

	close(importing.qrFeed)
	waitClosed(t, first, "the pairing with history to end")
}

func TestManagerPairWithoutHistoryJoinsAPairingInFlightWithoutHistory(t *testing.T) {
	plain := newFakeWAClient()
	plain.qrFeed = make(chan whatsmeow.QRChannelItem)
	unused := newFakeWAClient()
	mgr := managerOver(plain, unused)

	first, err := mgr.Pair(context.Background(), "channel-join-plain", false)
	if err != nil {
		t.Fatalf("Pair(without history) failed: %v", err)
	}
	select {
	case plain.qrFeed <- whatsmeow.QRChannelItem{Event: "code", Code: "qr-plain-join", Timeout: time.Minute}:
	case <-time.After(10 * time.Second):
		t.Fatal("the pairing never read its QR")
	}
	<-first

	second, err := mgr.Pair(context.Background(), "channel-join-plain", false)
	if err != nil {
		t.Fatalf("the second pairing without history must join, got %v", err)
	}
	if got := waitClosed(t, second, "the joined replay"); len(got) != 1 || got[0].QR != "qr-plain-join" {
		t.Fatalf("the second request must receive the QR in flight, got %+v", got)
	}
	if plain.disconnectCount() != 0 || plain.TakesOverHistory() || unused.connectCallCount() != 0 {
		t.Fatal("a request without history must never restart nor change the pairing in flight")
	}

	close(plain.qrFeed)
	waitClosed(t, first, "the pairing to end")
}
