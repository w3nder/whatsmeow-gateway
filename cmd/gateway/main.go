package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/w3nder/whatsmeow-gateway/internal/config"
	"github.com/w3nder/whatsmeow-gateway/internal/gateway"
	"github.com/w3nder/whatsmeow-gateway/internal/logging"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		_, _ = os.Stderr.WriteString(err.Error() + "\n")
		os.Exit(1)
	}

	waLogger, logger := logging.New()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := gateway.Run(ctx, cfg, waLogger, logger); err != nil {
		logger.Error("gateway run failed", "error", err)
		os.Exit(1)
	}
}
