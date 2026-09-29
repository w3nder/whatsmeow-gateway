package channelsettings_test

import (
	"testing"

	"github.com/w3nder/whatsmeow-gateway/internal/channelsettings"
)

func TestDefaultsKeepTodaysBehaviour(t *testing.T) {
	got := channelsettings.Defaults()
	if !got.ListenGroups || !got.ReceiveCalls || got.CallRejectMessage != "" {
		t.Fatalf("defaults must listen to groups, receive calls and carry no message, got %+v", got)
	}
}
