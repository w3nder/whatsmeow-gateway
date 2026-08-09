package test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/w3nder/whatsmeow-gateway/internal/session"
)

// drainPairUpdates collects everything a pair session reports, so a test can assert on
// the whole sequence rather than on whichever update happened to arrive first.
func drainPairUpdates(t *testing.T, updates <-chan session.PairUpdate, timeout time.Duration) []session.PairUpdate {
	t.Helper()

	var got []session.PairUpdate
	deadline := time.After(timeout)
	for {
		select {
		case u, ok := <-updates:
			if !ok {
				return got
			}
			got = append(got, u)
		case <-deadline:
			t.Fatalf("timed out draining pair updates, got %+v so far", got)
		}
	}
}

func qrCodes(updates []session.PairUpdate) []string {
	var codes []string
	for _, u := range updates {
		if u.QR != "" {
			codes = append(codes, u.QR)
		}
	}
	return codes
}

// TestManagerPairReplaysStillValidQRToSecondRequest is the reopened dialog: the operator
// closes the pairing dialog and opens it again while the code on the wire is still good.
// The second command must be answered with that code, not with silence until whatsmeow
// rotates.
func TestManagerPairReplaysStillValidQRToSecondRequest(t *testing.T) {
	fake := newFakeWAClient()
	fake.qrFeed = make(chan whatsmeow.QRChannelItem)

	factoryCalls := 0
	mgr := session.NewManager(func(channelID string, jid *types.JID) (session.WAClient, error) {
		factoryCalls++
		return fake, nil
	})

	first, err := mgr.Pair(context.Background(), "channel-replay-1")
	if err != nil {
		t.Fatalf("first Pair failed: %v", err)
	}

	fake.qrFeed <- whatsmeow.QRChannelItem{Event: "code", Code: "qr-live", Timeout: time.Minute}
	if got := <-first; got.QR != "qr-live" {
		t.Fatalf("expected the first pair session to report qr-live, got %+v", got)
	}

	second, err := mgr.Pair(context.Background(), "channel-replay-1")
	if err != nil {
		t.Fatalf("second Pair failed: %v", err)
	}

	replayed := drainPairUpdates(t, second, 2*time.Second)
	if codes := qrCodes(replayed); len(codes) != 1 || codes[0] != "qr-live" {
		t.Fatalf("expected the second pair command to be answered with the still-valid code immediately, got %+v", replayed)
	}

	if factoryCalls != 1 {
		t.Fatalf("expected the second pair command to reuse the pairing client, factory called %d times", factoryCalls)
	}
	if fake.qrChannelCallCount() != 1 {
		t.Fatalf("expected a single QR loop for the channel, got %d QRChannel calls", fake.qrChannelCallCount())
	}

	close(fake.qrFeed)
	drainPairUpdates(t, first, 2*time.Second)
}

// TestManagerPairDoesNotReplayExpiredQR pins the other half of replaying: a code whose
// window has passed is dead, and handing it back would send the operator to scan
// something WhatsApp refuses with no explanation.
func TestManagerPairDoesNotReplayExpiredQR(t *testing.T) {
	fake := newFakeWAClient()
	fake.qrFeed = make(chan whatsmeow.QRChannelItem)

	mgr := session.NewManager(func(channelID string, jid *types.JID) (session.WAClient, error) {
		return fake, nil
	})

	first, err := mgr.Pair(context.Background(), "channel-expired-1")
	if err != nil {
		t.Fatalf("first Pair failed: %v", err)
	}

	const codeLifetime = 80 * time.Millisecond
	fake.qrFeed <- whatsmeow.QRChannelItem{Event: "code", Code: "qr-expiring", Timeout: codeLifetime}
	if got := <-first; got.QR != "qr-expiring" {
		t.Fatalf("expected the first pair session to report qr-expiring, got %+v", got)
	}

	time.Sleep(2 * codeLifetime)

	second, err := mgr.Pair(context.Background(), "channel-expired-1")
	if err != nil {
		t.Fatalf("second Pair failed: %v", err)
	}

	if replayed := drainPairUpdates(t, second, 2*time.Second); len(replayed) != 0 {
		t.Fatalf("expected an expired code never to be replayed, got %+v", replayed)
	}

	close(fake.qrFeed)
	drainPairUpdates(t, first, 2*time.Second)
}

// TestManagerPairAfterTimeoutPairsFromCleanClient covers the retry after a failed
// pairing: the timed-out client is gone, the next command builds a fresh device, and the
// dead code from the previous attempt is never replayed.
func TestManagerPairAfterTimeoutPairsFromCleanClient(t *testing.T) {
	timedOut := newFakeWAClient()
	timedOut.qrItems = []whatsmeow.QRChannelItem{
		{Event: "code", Code: "qr-before-timeout", Timeout: time.Minute},
		whatsmeow.QRChannelTimeout,
	}
	retry := newFakeWAClient()
	retry.qrItems = []whatsmeow.QRChannelItem{
		{Event: "code", Code: "qr-after-timeout", Timeout: time.Minute},
	}

	factoryCalls := 0
	mgr := session.NewManager(func(channelID string, jid *types.JID) (session.WAClient, error) {
		factoryCalls++
		if factoryCalls == 1 {
			return timedOut, nil
		}
		return retry, nil
	})

	const channelID = "channel-timeout-retry-1"

	firstRun := drainPairUpdates(t, mustPair(t, mgr, channelID), 2*time.Second)
	if len(firstRun) != 2 || firstRun[0].QR != "qr-before-timeout" || firstRun[1].Err == nil {
		t.Fatalf("expected a code followed by the timeout error, got %+v", firstRun)
	}

	if timedOut.disconnectCount() == 0 {
		t.Fatal("expected the timed-out client's socket to be closed rather than left half-dead in the session map")
	}
	if err := mgr.EnsureConnected(channelID); !errors.Is(err, session.ErrNoSession) {
		t.Fatalf("expected the timed-out channel to hold no session at all, got %v", err)
	}

	secondRun := drainPairUpdates(t, mustPair(t, mgr, channelID), 2*time.Second)
	codes := qrCodes(secondRun)
	if len(codes) != 1 || codes[0] != "qr-after-timeout" {
		t.Fatalf("expected the retry to produce a fresh code straight away, got %+v", secondRun)
	}
	if factoryCalls != 2 {
		t.Fatalf("expected the retry to build a fresh device, factory called %d times", factoryCalls)
	}
}

// TestManagerPairAfterErrorDoesNotReplayQR is the same rule for the outcomes whatsmeow
// reports as errors rather than as a timeout -- and those leave the socket up, which is
// what makes reusing the client fail outright instead of merely being stale.
func TestManagerPairAfterErrorDoesNotReplayQR(t *testing.T) {
	failed := newFakeWAClient()
	failed.qrItems = []whatsmeow.QRChannelItem{
		{Event: "code", Code: "qr-before-error", Timeout: time.Minute},
		{Event: "error", Error: errors.New("pairing refused")},
	}
	retry := newFakeWAClient()
	retry.qrItems = []whatsmeow.QRChannelItem{
		{Event: "code", Code: "qr-after-error", Timeout: time.Minute},
	}

	factoryCalls := 0
	mgr := session.NewManager(func(channelID string, jid *types.JID) (session.WAClient, error) {
		factoryCalls++
		if factoryCalls == 1 {
			return failed, nil
		}
		return retry, nil
	})

	const channelID = "channel-error-retry-1"

	firstRun := drainPairUpdates(t, mustPair(t, mgr, channelID), 2*time.Second)
	if len(firstRun) != 2 || firstRun[1].Err == nil {
		t.Fatalf("expected a code followed by the pairing error, got %+v", firstRun)
	}

	secondRun := drainPairUpdates(t, mustPair(t, mgr, channelID), 2*time.Second)
	codes := qrCodes(secondRun)
	if len(codes) != 1 || codes[0] != "qr-after-error" {
		t.Fatalf("expected the retry after an error to produce its own fresh code, got %+v", secondRun)
	}
}

// TestManagerPairAfterSuccessRetainsNoQR: once the channel is paired, the last code is
// meaningless. A pair command for a paired channel reports the connection and nothing
// else -- least of all a QR the operator might try to scan.
func TestManagerPairAfterSuccessRetainsNoQR(t *testing.T) {
	paired := newFakeWAClient()
	paired.qrFeed = make(chan whatsmeow.QRChannelItem)
	relinked := newFakeWAClient()
	relinked.qrItems = []whatsmeow.QRChannelItem{
		{Event: "code", Code: "qr-after-relink", Timeout: time.Minute},
	}

	factoryCalls := 0
	mgr := session.NewManager(func(channelID string, jid *types.JID) (session.WAClient, error) {
		factoryCalls++
		if factoryCalls == 1 {
			return paired, nil
		}
		return relinked, nil
	})

	fake := paired
	const channelID = "channel-paired-1"

	first, err := mgr.Pair(context.Background(), channelID)
	if err != nil {
		t.Fatalf("first Pair failed: %v", err)
	}

	fake.qrFeed <- whatsmeow.QRChannelItem{Event: "code", Code: "qr-scanned", Timeout: time.Minute}
	if got := <-first; got.QR != "qr-scanned" {
		t.Fatalf("expected the pair session to report qr-scanned, got %+v", got)
	}

	fake.markPaired()
	fake.qrFeed <- whatsmeow.QRChannelSuccess
	if got := <-first; !got.Connected {
		t.Fatalf("expected the pair session to report the successful pair, got %+v", got)
	}
	close(fake.qrFeed)
	drainPairUpdates(t, first, 2*time.Second)

	second := drainPairUpdates(t, mustPair(t, mgr, channelID), 2*time.Second)
	if len(second) != 1 || !second[0].Connected {
		t.Fatalf("expected a pair command for a paired channel to report only the connection, got %+v", second)
	}

	// Unlinking from the phone sends the channel back through the QR flow. Whatever the
	// successful pairing left behind must not follow it there.
	paired.emit(&events.LoggedOut{})
	waitForNoSession(t, mgr, channelID)

	third := drainPairUpdates(t, mustPair(t, mgr, channelID), 2*time.Second)
	codes := qrCodes(third)
	if len(codes) != 1 || codes[0] != "qr-after-relink" {
		t.Fatalf("expected the relink to show its own fresh code, got %+v", third)
	}
}

// waitForNoSession blocks until the manager has let go of a channel. Dropping a session
// runs off the client's event handler, so it is not done the instant the event is
// emitted.
func waitForNoSession(t *testing.T, mgr *session.Manager, channelID string) {
	t.Helper()

	deadline := time.After(2 * time.Second)
	for {
		if errors.Is(mgr.EnsureConnected(channelID), session.ErrNoSession) {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for channel %s to be dropped", channelID)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TestManagerConcurrentPairRequestsRunOneQRLoop: two commands landing together must not
// leave two loops fighting over the same client and publishing every rotation twice.
func TestManagerConcurrentPairRequestsRunOneQRLoop(t *testing.T) {
	fake := newFakeWAClient()
	fake.qrFeed = make(chan whatsmeow.QRChannelItem)

	factoryCalls := 0
	var factoryMu sync.Mutex
	mgr := session.NewManager(func(channelID string, jid *types.JID) (session.WAClient, error) {
		factoryMu.Lock()
		factoryCalls++
		factoryMu.Unlock()
		return fake, nil
	})

	const channelID = "channel-concurrent-1"

	var (
		started   sync.WaitGroup
		finished  sync.WaitGroup
		countMu   sync.Mutex
		published []string
	)
	started.Add(2)
	finished.Add(2)

	// Both callers must have claimed (or failed to claim) the channel before any code
	// is fed, so the assertion below counts rotations only -- never a replay that the
	// scheduler happened to allow.
	sessions := make(chan (<-chan session.PairUpdate), 2)
	for range 2 {
		go func() {
			updates, err := mgr.Pair(context.Background(), channelID)
			if err != nil {
				t.Errorf("concurrent Pair failed: %v", err)
				sessions <- nil
				started.Done()
				finished.Done()
				return
			}
			sessions <- updates
			started.Done()

			defer finished.Done()
			for u := range updates {
				if u.QR == "" {
					continue
				}
				countMu.Lock()
				published = append(published, u.QR)
				countMu.Unlock()
			}
		}()
	}
	started.Wait()

	fake.qrFeed <- whatsmeow.QRChannelItem{Event: "code", Code: "qr-rot-1", Timeout: time.Minute}
	fake.qrFeed <- whatsmeow.QRChannelItem{Event: "code", Code: "qr-rot-2", Timeout: time.Minute}
	close(fake.qrFeed)

	done := make(chan struct{})
	go func() {
		finished.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the concurrent pair sessions to finish")
	}
	close(sessions)

	if fake.qrChannelCallCount() != 1 {
		t.Fatalf("expected one QR loop for the channel, got %d QRChannel calls", fake.qrChannelCallCount())
	}
	if fake.connectCallCount() != 1 {
		t.Fatalf("expected one connect for the channel, got %d", fake.connectCallCount())
	}
	if factoryCalls != 1 {
		t.Fatalf("expected both commands to share one client, factory called %d times", factoryCalls)
	}

	countMu.Lock()
	defer countMu.Unlock()
	if len(published) != 2 {
		t.Fatalf("expected each rotation to be reported exactly once, got %v", published)
	}
}

func mustPair(t *testing.T, mgr *session.Manager, channelID string) <-chan session.PairUpdate {
	t.Helper()

	updates, err := mgr.Pair(context.Background(), channelID)
	if err != nil {
		t.Fatalf("Pair(%s) failed: %v", channelID, err)
	}
	return updates
}
