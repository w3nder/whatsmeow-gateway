package gateway

import (
	"context"
	"log/slog"

	"github.com/w3nder/whatsmeow-gateway/internal/config"
	waLog "go.mau.fi/whatsmeow/util/log"
)

func Run(ctx context.Context, cfg config.Config, waLogger waLog.Logger, logger *slog.Logger) error {
	logger.Info("started", "instance_id", cfg.InstanceID)
	<-ctx.Done()
	logger.Info("stopping")
	return nil
}
