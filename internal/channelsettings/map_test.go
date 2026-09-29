package channelsettings_test

import (
	"sync"
	"testing"

	"github.com/w3nder/whatsmeow-gateway/internal/channelsettings"
)

func TestMapReturnsTheDefaultsForAnUnknownChannel(t *testing.T) {
	m := channelsettings.NewMap()
	if got := m.For("channel-1"); got != channelsettings.Defaults() {
		t.Fatalf("unknown channel = %+v, want the defaults", got)
	}
}

func TestMapReturnsWhatWasSetPerChannel(t *testing.T) {
	m := channelsettings.NewMap()
	muted := channelsettings.Settings{ListenGroups: false, ReceiveCalls: false, CallRejectMessage: "texto"}
	m.Set("channel-1", muted)

	if got := m.For("channel-1"); got != muted {
		t.Fatalf("channel-1 = %+v, want %+v", got, muted)
	}
	if got := m.For("channel-2"); got != channelsettings.Defaults() {
		t.Fatalf("channel-2 must keep the defaults, got %+v", got)
	}
}

func TestMapClearGoesBackToTheDefaults(t *testing.T) {
	m := channelsettings.NewMap()
	m.Set("channel-1", channelsettings.Settings{})
	m.Clear("channel-1")

	if got := m.For("channel-1"); got != channelsettings.Defaults() {
		t.Fatalf("cleared channel = %+v, want the defaults", got)
	}
}

func TestMapIsSafeForConcurrentUse(t *testing.T) {
	m := channelsettings.NewMap()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			m.Set("channel-1", channelsettings.Settings{ReceiveCalls: true})
		}()
		go func() {
			defer wg.Done()
			_ = m.For("channel-1")
		}()
	}
	wg.Wait()
}
