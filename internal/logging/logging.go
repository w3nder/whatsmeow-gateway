package logging

import (
	"log/slog"
	"os"

	waLog "go.mau.fi/whatsmeow/util/log"
)

func New() (waLog.Logger, *slog.Logger) {
	waLogger := waLog.Stdout("gateway", "INFO", false)
	slogger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	return waLogger, slogger
}
