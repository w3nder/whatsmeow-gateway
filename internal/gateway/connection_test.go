package gateway

import (
	"testing"

	"go.mau.fi/whatsmeow/types/events"
)

func TestOnlyTerminalSessionEventsEndLiveCalls(t *testing.T) {
	for _, tc := range []struct {
		name       string
		evt        any
		wantDead   bool
		wantReason string
	}{
		{"disconnected", &events.Disconnected{}, false, ""},
		{"keepalive timeout", &events.KeepAliveTimeout{}, false, ""},
		{"keepalive restored", &events.KeepAliveRestored{}, false, ""},
		{"connected", &events.Connected{}, false, ""},
		{"stream error", &events.StreamError{}, false, ""},
		{"temporary ban", &events.TemporaryBan{}, false, ""},
		{"message", &events.Message{}, false, ""},

		{"logged out", &events.LoggedOut{}, true, "logged_out"},
		{"stream replaced", &events.StreamReplaced{}, true, "stream_replaced"},
		{"client outdated", &events.ClientOutdated{}, true, "client_outdated"},
		{"connect failure", &events.ConnectFailure{}, true, "connect_failure"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reason, dead := callsAreDead(tc.evt)
			if dead != tc.wantDead {
				t.Errorf("callsAreDead = %v, want %v", dead, tc.wantDead)
			}
			if reason != tc.wantReason {
				t.Errorf("reason = %q, want %q", reason, tc.wantReason)
			}
		})
	}
}
