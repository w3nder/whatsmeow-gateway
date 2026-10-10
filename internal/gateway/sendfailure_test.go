package gateway

import (
	"errors"
	"fmt"
	"testing"

	"github.com/w3nder/whatsmeow-gateway/internal/session"
)

func TestSendFailureCodeClassifiesByTypedError(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"no session", session.ErrNoSession, "session_missing"},
		{"no session with channel", fmt.Errorf("%w: channel-1", session.ErrNoSession), "session_missing"},
		{"not paired wrapped by ensure connected", fmt.Errorf("gateway: ensure connected c: %w", fmt.Errorf("gateway: channel c is not paired: %w", fmt.Errorf("%w: c", session.ErrNoSession))), "session_missing"},
		{"socket down", session.ErrSocketDown, "session_down"},
		{"socket down with wait", fmt.Errorf("%w after 10s, auto-reconnect still running", session.ErrSocketDown), "session_down"},
		{"socket down wrapped", fmt.Errorf("gateway: ensure connected c: %w", fmt.Errorf("%w after 10s", session.ErrSocketDown)), "session_down"},
		{"whatsapp rejected", errors.New("boom: whatsapp rejected the message"), "gateway_send_error"},
		{"text that only mentions the session", errors.New("session: channel has no live session"), "gateway_send_error"},
		{"text that only mentions the socket", errors.New("session: socket still down after 10s"), "gateway_send_error"},
		{"wrapped generic", fmt.Errorf("gateway: send m: %w", errors.New("timeout")), "gateway_send_error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := sendFailureCode(tc.err); got != tc.want {
				t.Fatalf("sendFailureCode(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

func TestSessionErrorTextsStayUnchangedForTheBackendFallback(t *testing.T) {
	if got, want := fmt.Errorf("%w: c", session.ErrNoSession).Error(), "session: channel has no live session: c"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	if got, want := fmt.Errorf("%w after %s, auto-reconnect still running", session.ErrSocketDown, "10s").Error(), "session: socket still down after 10s, auto-reconnect still running"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
