package deviceprops_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waCompanionReg"
	"go.mau.fi/whatsmeow/store"
	"google.golang.org/protobuf/proto"

	"github.com/w3nder/whatsmeow-gateway/internal/deviceprops"
)

func currentProps() *waCompanionReg.DeviceProps {
	return proto.Clone(store.DeviceProps).(*waCompanionReg.DeviceProps)
}

func carriesHistory(props *waCompanionReg.DeviceProps) bool {
	config := props.GetHistorySyncConfig()
	return props.GetPlatformType() == deviceprops.HistoryPlatform &&
		props.GetRequireFullSync() == deviceprops.HistoryRequireFullSync &&
		config.GetFullSyncDaysLimit() == deviceprops.WindowDays &&
		config.GetRecentSyncDaysLimit() == deviceprops.WindowDays &&
		config.GetFullSyncSizeMbLimit() == deviceprops.HistorySizeMbLimit
}

func acquire(t *testing.T, importHistory bool) func() {
	t.Helper()
	release, err := deviceprops.Acquire(context.Background(), importHistory)
	if err != nil {
		t.Fatalf("Acquire(%v): %v", importHistory, err)
	}
	return release
}

func acquireInBackground(importHistory bool, seen chan<- *waCompanionReg.DeviceProps) chan func() {
	acquired := make(chan func(), 1)
	go func() {
		release, err := deviceprops.Acquire(context.Background(), importHistory)
		if err != nil {
			close(acquired)
			return
		}
		seen <- store.DeviceProps
		acquired <- release
	}()
	return acquired
}

func TestAcquireWithoutHistoryKeepsTodaysProps(t *testing.T) {
	before := store.DeviceProps
	defaults := currentProps()

	release := acquire(t, false)
	during := store.DeviceProps
	release()

	if during != before || !proto.Equal(during, defaults) || store.DeviceProps != before {
		t.Fatal("an opted-out pairing must run with exactly today's props and never touch them")
	}
}

func TestAcquireWithHistoryHoldsTheHistoryPropsUntilTheRelease(t *testing.T) {
	before := store.DeviceProps
	defaults := currentProps()

	release := acquire(t, true)
	if !carriesHistory(store.DeviceProps) {
		t.Fatalf("the pairing that imports history must run with the history props, got %v", store.DeviceProps)
	}
	release()

	if store.DeviceProps != before || !proto.Equal(store.DeviceProps, defaults) {
		t.Fatal("the release must restore the original props untouched")
	}
}

func TestReleaseIsIdempotent(t *testing.T) {
	release := acquire(t, true)
	release()
	release()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	again, err := deviceprops.Acquire(ctx, true)
	if err != nil {
		t.Fatalf("a double release must neither block nor leak the slot, got %v", err)
	}
	again()
}

func TestPairingsWithoutHistoryRunTogether(t *testing.T) {
	first := acquire(t, false)
	defer first()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	second, err := deviceprops.Acquire(ctx, false)
	if err != nil {
		t.Fatalf("a pairing without history must not wait for another one without history, got %v", err)
	}
	second()
}

func TestAPairingWaitsForAHistoryPairingToFinish(t *testing.T) {
	defaults := currentProps()
	importing := acquire(t, true)

	seen := make(chan *waCompanionReg.DeviceProps, 1)
	acquired := acquireInBackground(false, seen)
	select {
	case <-acquired:
		t.Fatal("no pairing may start while a history pairing holds the props")
	case <-time.After(150 * time.Millisecond):
	}

	importing()
	release, ok := <-acquired
	if !ok {
		t.Fatal("the waiting pairing must get the slot after the history pairing ends")
	}
	defer release()
	if props := <-seen; !proto.Equal(props, defaults) {
		t.Fatalf("the pairing after a history pairing must see today's props, got %v", props)
	}
}

func TestAHistoryPairingWaitsForThePairingsInFlight(t *testing.T) {
	plain := acquire(t, false)

	seen := make(chan *waCompanionReg.DeviceProps, 1)
	acquired := acquireInBackground(true, seen)
	select {
	case <-acquired:
		t.Fatal("a history pairing must not swap the props under a pairing in flight")
	case <-time.After(150 * time.Millisecond):
	}

	plain()
	release, ok := <-acquired
	if !ok {
		t.Fatal("the history pairing must get the slot after the pairing in flight ends")
	}
	defer release()
	if props := <-seen; !carriesHistory(props) {
		t.Fatalf("the history pairing must run with the history props, got %v", props)
	}
}

func TestAcquireGivesUpWhenItsContextEnds(t *testing.T) {
	importing := acquire(t, true)
	defer importing()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := deviceprops.Acquire(ctx, false); err == nil {
		t.Fatal("a pairing whose command was cancelled must stop waiting for the slot")
	}
}

func TestConcurrentPairingsEachRunWithTheirOwnProps(t *testing.T) {
	before := store.DeviceProps
	defaults := currentProps()

	var wrong atomic.Int32
	var pairings sync.WaitGroup
	for n := range 32 {
		importHistory := n%4 == 0
		pairings.Add(1)
		go func() {
			defer pairings.Done()
			release, err := deviceprops.Acquire(context.Background(), importHistory)
			if err != nil {
				wrong.Add(1)
				return
			}
			defer release()
			props := store.DeviceProps
			time.Sleep(time.Millisecond)
			if props != store.DeviceProps || importHistory != carriesHistory(props) || (!importHistory && !proto.Equal(props, defaults)) {
				wrong.Add(1)
			}
		}()
	}
	pairings.Wait()

	if wrong.Load() != 0 {
		t.Fatalf("%d pairings saw another pairing's props", wrong.Load())
	}
	if store.DeviceProps != before {
		t.Fatal("the process-wide props must be the original ones after every pairing")
	}
}
