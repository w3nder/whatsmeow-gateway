package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"

	"github.com/rs/zerolog"
)

func NewCallLogger(logger *slog.Logger, channelID string) zerolog.Logger {
	bridge := &zerologBridge{logger: logger.With("channel_id", channelID)}
	return zerolog.New(bridge)
}

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
