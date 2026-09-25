package config_test

import (
	"testing"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/config"
)

func requiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("GATEWAY_INSTANCE_ID", "gw-1")
	t.Setenv("AMQP_URL", "amqp://localhost")
	t.Setenv("SESSION_DATABASE_URL", "postgres://localhost/db")
	t.Setenv("REDIS_URL", "redis://localhost")
}

func TestGroupPrefetchDefaultsAndReadsTheEnvironment(t *testing.T) {
	requiredEnv(t)
	cfg, err := config.Load()
	if err != nil || cfg.GroupPrefetch != amqp.DefaultGroupPrefetch {
		t.Fatalf("default group prefetch %d (%v)", cfg.GroupPrefetch, err)
	}
	t.Setenv("GROUP_PREFETCH", "128")
	if cfg, err := config.Load(); err != nil || cfg.GroupPrefetch != 128 {
		t.Fatalf("GROUP_PREFETCH=128 → %d (%v)", cfg.GroupPrefetch, err)
	}
	t.Setenv("GROUP_PREFETCH", "zero")
	if _, err := config.Load(); err == nil {
		t.Fatal("a non numeric GROUP_PREFETCH must be refused")
	}
}
