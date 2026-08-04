package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"

	"github.com/rs/zerolog"
)

// This file exists because meowcaller (the calling library) logs exclusively through
// zerolog and defaults to zerolog.Nop() when no logger is supplied. Without wiring one
// in, the library -- including its own startup failures, which it reports only by
// logging (e.g. "construct the client before connecting whatsmeow") -- is completely
// silent. That silence is indistinguishable from a call never arriving at all. The
// bridge below re-emits every line meowcaller logs through the gateway's slog output,
// so one log stream and one level control (the gateway's) governs both.

// NewCallLogger returns a zerolog.Logger that forwards everything it logs into logger,
// tagged with the channel the calling client belongs to. Pass the result to
// meowcaller.WithLogger. The returned logger is left at zerolog's default (trace-and-up)
// level so filtering happens once, in the gateway's slog handler, rather than twice.
func NewCallLogger(logger *slog.Logger, channelID string) zerolog.Logger {
	bridge := &zerologBridge{logger: logger.With("channel_id", channelID)}
	return zerolog.New(bridge)
}

// zerologBridge implements zerolog.LevelWriter. zerolog calls WriteLevel (not Write)
// whenever the writer supports it, handing over the level it already resolved -- that
// is more reliable than re-deriving it from the encoded JSON, so the bridge only parses
// the line for the message and any extra fields worth keeping.
type zerologBridge struct {
	logger *slog.Logger
}

func (b *zerologBridge) Write(p []byte) (int, error) {
	return b.WriteLevel(zerolog.NoLevel, p)
}

func (b *zerologBridge) WriteLevel(level zerolog.Level, p []byte) (int, error) {
	msg, attrs := parseZerologLine(p)
	b.logger.LogAttrs(context.Background(), slogLevel(level), msg, attrs...)
	return len(p), nil
}

// parseZerologLine pulls the message and any extra fields out of one zerolog JSON line.
// time and level are dropped: slog stamps its own record time, and the level already
// came through WriteLevel. A line that fails to decode -- not expected against
// zerolog's own encoder, but a library upgrade could change the wire format -- still
// reaches the gateway's logs verbatim instead of vanishing.
func parseZerologLine(p []byte) (string, []slog.Attr) {
	var fields map[string]any
	if err := json.Unmarshal(p, &fields); err != nil {
		return string(bytes.TrimRight(p, "\n")), nil
	}

	msg, _ := fields[zerolog.MessageFieldName].(string)
	delete(fields, zerolog.MessageFieldName)
	delete(fields, zerolog.LevelFieldName)
	delete(fields, zerolog.TimestampFieldName)

	if len(fields) == 0 {
		return msg, nil
	}
	attrs := make([]slog.Attr, 0, len(fields))
	for k, v := range fields {
		attrs = append(attrs, slog.Any(k, v))
	}
	return msg, attrs
}

// slogLevel maps zerolog's level scale onto slog's. meowcaller logs a lot at debug and
// trace, so both fold to slog.LevelDebug rather than being forced up to Info: the
// gateway's existing handler level is what should decide whether they show up, not this
// bridge.
func slogLevel(level zerolog.Level) slog.Level {
	switch level {
	case zerolog.TraceLevel, zerolog.DebugLevel:
		return slog.LevelDebug
	case zerolog.InfoLevel:
		return slog.LevelInfo
	case zerolog.WarnLevel:
		return slog.LevelWarn
	case zerolog.ErrorLevel, zerolog.FatalLevel, zerolog.PanicLevel:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
