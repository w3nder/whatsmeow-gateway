package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
	rabbitmq "github.com/rabbitmq/amqp091-go"
	goredis "github.com/redis/go-redis/v9"
	waLog "go.mau.fi/whatsmeow/util/log"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/call"
	"github.com/w3nder/whatsmeow-gateway/internal/config"
	"github.com/w3nder/whatsmeow-gateway/internal/dedupe"
	"github.com/w3nder/whatsmeow-gateway/internal/gateway"
	"github.com/w3nder/whatsmeow-gateway/internal/logging"
	"github.com/w3nder/whatsmeow-gateway/internal/media"
	"github.com/w3nder/whatsmeow-gateway/internal/ownership"
	"github.com/w3nder/whatsmeow-gateway/internal/registry"
	"github.com/w3nder/whatsmeow-gateway/internal/session"
	"github.com/w3nder/whatsmeow-gateway/internal/store"
)

func main() {
	_ = godotenv.Load()

	cfg, err := config.Load()
	if err != nil {
		_, _ = os.Stderr.WriteString(err.Error() + "\n")
		os.Exit(1)
	}

	waLogger, logger := logging.New()
	slog.SetDefault(logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		logger.Info("shutdown signal received, draining")
		cancel()
		<-sigCh
		logger.Warn("second shutdown signal, forcing exit")
		os.Exit(1)
	}()

	if err := run(ctx, cfg, waLogger, logger); err != nil {
		logger.Error("gateway run failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config.Config, waLogger waLog.Logger, logger *slog.Logger) error {
	conn, err := rabbitmq.Dial(cfg.AMQPURL)
	if err != nil {
		return fmt.Errorf("main: dial rabbitmq: %w", err)
	}
	defer func() {
		if err := conn.Close(); err != nil && err != rabbitmq.ErrClosed {
			logger.Error("main: close rabbitmq connection", "error", err)
		}
	}()

	consumer, err := amqp.NewConsumer(conn, amqp.ConsumerConfig{Prefetch: cfg.Prefetch})
	if err != nil {
		return fmt.Errorf("main: new consumer: %w", err)
	}

	publisher, err := amqp.NewPublisher(conn)
	if err != nil {
		return fmt.Errorf("main: new publisher: %w", err)
	}
	defer func() {
		if err := publisher.Close(); err != nil {
			logger.Error("main: close publisher", "error", err)
		}
	}()

	redisOpts, err := goredis.ParseURL(cfg.RedisURL)
	if err != nil {
		return fmt.Errorf("main: parse redis url: %w", err)
	}
	redisClient := goredis.NewClient(redisOpts)
	defer func() {
		if err := redisClient.Close(); err != nil {
			logger.Error("main: close redis client", "error", err)
		}
	}()

	ownershipStore := ownership.NewStore(redisClient, cfg.ShardCount)

	sessionContainer, err := store.Open(ctx, cfg.SessionDSN, waLogger)
	if err != nil {
		return fmt.Errorf("main: open session store: %w", err)
	}

	dedupeStore, err := dedupe.Open(ctx, cfg.SessionDSN)
	if err != nil {
		return fmt.Errorf("main: open dedupe store: %w", err)
	}
	defer dedupeStore.Close()

	registryStore, err := registry.Open(ctx, cfg.SessionDSN)
	if err != nil {
		return fmt.Errorf("main: open registry store: %w", err)
	}
	defer registryStore.Close()

	mediaStore, err := media.NewS3Store(ctx, media.S3Config{
		Bucket:          cfg.S3Bucket,
		Region:          cfg.S3Region,
		Endpoint:        cfg.S3Endpoint,
		AccessKeyID:     cfg.S3AccessKeyID,
		SecretAccessKey: cfg.S3SecretAccessKey,
	})
	if err != nil {
		return fmt.Errorf("main: new media store: %w", err)
	}

	manager := session.NewManager(gateway.NewWAClientFactory(sessionContainer, waLogger))

	return gateway.Run(ctx, gateway.Deps{
		Consumer:             consumer,
		Publisher:            publisher,
		Manager:              manager,
		Ownership:            ownershipStore,
		Dedupe:               dedupeStore,
		Registry:             registryStore,
		MediaStore:           mediaStore,
		InstanceID:           cfg.InstanceID,
		ShardLockTTL:         cfg.ShardLockTTL,
		SendTimeout:          cfg.SendTimeout,
		ShutdownDrainTimeout: cfg.ShutdownDrainTimeout,
		CallOptions: call.Options{
			TmpDir: cfg.CallTmpDir,
			Record: cfg.CallRecord,
		},
		Logger: logger,
	})
}
