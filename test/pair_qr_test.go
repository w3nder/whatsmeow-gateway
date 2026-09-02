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

func TestManagerPairSkipsQRThatExpiredInWhatsmeowsBuffer(t *testing.T) {
	fake := newFakeWAClient()
	fake.qrFeed = make(chan whatsmeow.QRChannelItem)

	mgr := session.NewManager(func(channelID string, jid *types.JID) (session.WAClient, error) {
		return fake, nil
	})

	updates, err := mgr.Pair(context.Background(), "channel-stale-buffer-1")
	if err != nil {
		t.Fatalf("Pair failed: %v", err)
	}

	var (
		codesMu sync.Mutex
		codes   []string
		drained = make(chan struct{})
	)
	go func() {
		defer close(drained)
		for u := range updates {
			if u.QR == "" {
				continue
			}
			codesMu.Lock()
			codes = append(codes, u.QR)
			codesMu.Unlock()
		}
	}()
	published := func() []string {
		codesMu.Lock()
		defer codesMu.Unlock()
		return append([]string(nil), codes...)
	}

	const lifetime = 600 * time.Millisecond

	fake.qrFeed <- whatsmeow.QRChannelItem{Event: "code", Code: "qr-first", Timeout: lifetime}

	time.Sleep(lifetime * 5 / 2)

	fake.qrFeed <- whatsmeow.QRChannelItem{Event: "code", Code: "qr-expired-in-buffer", Timeout: lifetime}
	fake.qrFeed <- whatsmeow.QRChannelItem{Event: "code", Code: "qr-current", Timeout: lifetime}
	close(fake.qrFeed)

	select {
	case <-drained:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for the pair session to end, published %v", published())
	}

	got := published()
	if len(got) != 2 || got[0] != "qr-first" || got[1] != "qr-current" {
		t.Fatalf("expected the code that expired in the buffer to be dropped and only the live ones published, got %v", got)
	}
}

func TestManagerPairReplayJudgesTheRetainedQRFromWhenItWasEmitted(t *testing.T) {
	fake := newFakeWAClient()
	fake.qrFeed = make(chan whatsmeow.QRChannelItem)

	mgr := session.NewManager(func(channelID string, jid *types.JID) (session.WAClient, error) {
		return fake, nil
	})

	const channelID = "channel-late-read-1"

	first, err := mgr.Pair(context.Background(), channelID)
	if err != nil {
		t.Fatalf("first Pair failed: %v", err)
	}

	const lifetime = time.Second

	fake.qrFeed <- whatsmeow.QRChannelItem{Event: "code", Code: "qr-first", Timeout: lifetime}
	if got := <-first; got.QR != "qr-first" {
		t.Fatalf("expected the pair session to report qr-first, got %+v", got)
	}

	time.Sleep(lifetime * 3 / 2)
	fake.qrFeed <- whatsmeow.QRChannelItem{Event: "code", Code: "qr-half-spent", Timeout: lifetime}
	if got := <-first; got.QR != "qr-half-spent" {
		t.Fatalf("expected the part-spent code to still be published, got %+v", got)
	}

	replayed := drainPairUpdates(t, mustPair(t, mgr, channelID), 2*time.Second)
	if codes := qrCodes(replayed); len(codes) != 1 || codes[0] != "qr-half-spent" {
		t.Fatalf("expected the reopened dialog to get the code that is still on the wire, got %+v", replayed)
	}

	time.Sleep(lifetime * 7 / 10)
	if replayed := drainPairUpdates(t, mustPair(t, mgr, channelID), 2*time.Second); len(replayed) != 0 {
		t.Fatalf("expected a code past its own window never to be replayed, got %+v", replayed)
	}

	close(fake.qrFeed)
	drainPairUpdates(t, first, 2*time.Second)
}

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

	paired.emit(&events.LoggedOut{})
	waitForNoSession(t, mgr, channelID)

	third := drainPairUpdates(t, mustPair(t, mgr, channelID), 2*time.Second)
	codes := qrCodes(third)
	if len(codes) != 1 || codes[0] != "qr-after-relink" {
		t.Fatalf("expected the relink to show its own fresh code, got %+v", third)
	}
}

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
