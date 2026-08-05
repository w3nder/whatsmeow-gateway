package gateway

import (
	"testing"

	"go.mau.fi/whatsmeow/types/events"
)

// A transient socket drop used to end every live call on the channel and
// truncate its recording: whatsmeow raises Disconnected on every blip and then
// reconnects, while the call's media rides a relay socket that the blip never
// touched. The call was still up, the peer still talking, when the gateway
// published its `ended`, finished the recording and dropped it from the
// registry -- leaving a live call no hangup could reach.
//
// This test is the policy itself. It is worth having in this shape because the
// distinction is not observable from the event's name: Disconnected and
// ConnectFailure look equally alarming and mean opposite things.
func TestOnlyTerminalSessionEventsEndLiveCalls(t *testing.T) {
	for _, tc := range []struct {
		name       string
		evt        any
		wantDead   bool
		wantReason string
	}{
		// Recoverable: whatsmeow's own auto-reconnect is already handling
		// these, and the call is still up while it does.
		{"disconnected", &events.Disconnected{}, false, ""},
		{"keepalive timeout", &events.KeepAliveTimeout{}, false, ""},
		{"keepalive restored", &events.KeepAliveRestored{}, false, ""},
		{"connected", &events.Connected{}, false, ""},
		{"stream error", &events.StreamError{}, false, ""},
		{"temporary ban", &events.TemporaryBan{}, false, ""},
		// Ordinary traffic must obviously never end a call either.
		{"message", &events.Message{}, false, ""},

		// Terminal: whatsmeow does not come back from any of these, so a call
		// still registered on the channel is genuinely over.
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
