package logging_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/rs/zerolog"

	"github.com/w3nder/whatsmeow-gateway/internal/logging"
)

// TestNewCallLoggerPreservesLevelAndMessage is the pure, testable half of the fix: a
// line the calling library logs at a given zerolog level must come out on the gateway's
// slog logger at the matching level, with its message intact. The wiring that actually
// constructs a real meowcaller client is not something worth faking here.
func TestNewCallLoggerPreservesLevelAndMessage(t *testing.T) {
	cases := []struct {
		name      string
		log       func(l zerolog.Logger, msg string)
		wantLevel string
	}{
		{"trace", func(l zerolog.Logger, msg string) { l.Trace().Msg(msg) }, "DEBUG"},
		{"debug", func(l zerolog.Logger, msg string) { l.Debug().Msg(msg) }, "DEBUG"},
		{"info", func(l zerolog.Logger, msg string) { l.Info().Msg(msg) }, "INFO"},
		{"warn", func(l zerolog.Logger, msg string) { l.Warn().Msg(msg) }, "WARN"},
		{"error", func(l zerolog.Logger, msg string) { l.Error().Msg(msg) }, "ERROR"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			slogger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

			callLogger := logging.NewCallLogger(slogger, "channel-123")
			const msg = "raw call adapter is unavailable"
			tc.log(callLogger, msg)

			if buf.Len() == 0 {
				t.Fatalf("expected a log line for level %s, got nothing", tc.name)
			}

			var out map[string]any
			if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
				t.Fatalf("unmarshal slog output: %v\noutput: %s", err, buf.String())
			}

			if got := out["level"]; got != tc.wantLevel {
				t.Errorf("level = %v, want %s", got, tc.wantLevel)
			}
			if got := out["msg"]; got != msg {
				t.Errorf("msg = %v, want %q", got, msg)
			}
			if got := out["channel_id"]; got != "channel-123" {
				t.Errorf("channel_id = %v, want channel-123", got)
			}
		})
	}
}

// TestNewCallLoggerCarriesExtraFields checks that structured fields the library attaches
// (e.g. call_id) survive the bridge instead of being silently dropped, since those are
// exactly what makes a call's log lines useful for diagnosis.
func TestNewCallLoggerCarriesExtraFields(t *testing.T) {
	var buf bytes.Buffer
	slogger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	callLogger := logging.NewCallLogger(slogger, "channel-abc")
	callLogger.Info().Str("call_id", "call-1").Msg("offer sent")

	var out map[string]any
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatalf("unmarshal slog output: %v\noutput: %s", err, buf.String())
	}

	if got := out["call_id"]; got != "call-1" {
		t.Errorf("call_id = %v, want call-1", got)
	}
}

// TestNewCallLoggerBelowHandlerLevelIsFiltered confirms trace/debug from the library
// stay silent by default: the bridge maps them to slog.LevelDebug and lets the
// gateway's existing handler level decide whether they surface, rather than forcing
// everything up to Info.
func TestNewCallLoggerBelowHandlerLevelIsFiltered(t *testing.T) {
	var buf bytes.Buffer
	slogger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	callLogger := logging.NewCallLogger(slogger, "channel-xyz")
	callLogger.Debug().Msg("chatty diagnostic")

	if buf.Len() != 0 {
		t.Fatalf("expected debug line to be filtered at Info level, got: %s", buf.String())
	}
}
