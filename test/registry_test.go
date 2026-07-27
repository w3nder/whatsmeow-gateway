package test

import (
	"context"
	"testing"

	"github.com/w3nder/whatsmeow-gateway/internal/registry"
)

func TestRegistryStoreSaveThenForShardsReturnsOwnedSessions(t *testing.T) {
	dsn := startPostgresForGateway(t)
	ctx := context.Background()

	store, err := registry.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("registry.Open failed: %v", err)
	}
	t.Cleanup(store.Close)

	if err := store.Save(ctx, "channel-owned", "15551234567.0:1@s.whatsapp.net", "tenant-1"); err != nil {
		t.Fatalf("Save(channel-owned) failed: %v", err)
	}
	if err := store.Save(ctx, "channel-foreign", "15559876543.0:1@s.whatsapp.net", "tenant-2"); err != nil {
		t.Fatalf("Save(channel-foreign) failed: %v", err)
	}

	shardOf := map[string]int{"channel-owned": 0, "channel-foreign": 1}
	shardFn := func(channelID string) int { return shardOf[channelID] }

	sessions, err := store.ForShards(ctx, []int{0}, shardFn)
	if err != nil {
		t.Fatalf("ForShards failed: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("expected 1 owned session, got %d: %+v", len(sessions), sessions)
	}
	if sessions[0].ChannelID != "channel-owned" {
		t.Fatalf("expected channel-owned, got %+v", sessions[0])
	}
	if sessions[0].JID != "15551234567.0:1@s.whatsapp.net" {
		t.Fatalf("unexpected jid: %+v", sessions[0])
	}
	if sessions[0].TenantID != "tenant-1" {
		t.Fatalf("unexpected tenant id: %+v", sessions[0])
	}
}

func TestRegistryStoreSaveUpsertsOnConflict(t *testing.T) {
	dsn := startPostgresForGateway(t)
	ctx := context.Background()

	store, err := registry.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("registry.Open failed: %v", err)
	}
	t.Cleanup(store.Close)

	if err := store.Save(ctx, "channel-1", "jid-old@s.whatsapp.net", "tenant-old"); err != nil {
		t.Fatalf("first Save failed: %v", err)
	}
	if err := store.Save(ctx, "channel-1", "jid-new@s.whatsapp.net", "tenant-new"); err != nil {
		t.Fatalf("second Save failed: %v", err)
	}

	sessions, err := store.ForShards(ctx, []int{0}, func(string) int { return 0 })
	if err != nil {
		t.Fatalf("ForShards failed: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("expected the upsert to keep exactly 1 row for channel-1, got %d: %+v", len(sessions), sessions)
	}
	if sessions[0].JID != "jid-new@s.whatsapp.net" || sessions[0].TenantID != "tenant-new" {
		t.Fatalf("expected the second Save to overwrite jid/tenant, got %+v", sessions[0])
	}
}

func TestRegistryStoreDeleteRemovesRowSoItIsNotResumed(t *testing.T) {
	dsn := startPostgresForGateway(t)
	ctx := context.Background()

	store, err := registry.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("registry.Open failed: %v", err)
	}
	t.Cleanup(store.Close)

	if err := store.Save(ctx, "channel-logged-out", "jid@s.whatsapp.net", "tenant-1"); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	sessions, err := store.ForShards(ctx, []int{0}, func(string) int { return 0 })
	if err != nil {
		t.Fatalf("ForShards (before delete) failed: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("expected the saved row to be present before Delete, got %+v", sessions)
	}

	if err := store.Delete(ctx, "channel-logged-out"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	sessions, err = store.ForShards(ctx, []int{0}, func(string) int { return 0 })
	if err != nil {
		t.Fatalf("ForShards (after delete) failed: %v", err)
	}
	if len(sessions) != 0 {
		t.Fatalf("expected Delete to remove the row so it is never resumed, got %+v", sessions)
	}
}

func TestRegistryStoreDeleteOfUnknownChannelIsNoop(t *testing.T) {
	dsn := startPostgresForGateway(t)
	ctx := context.Background()

	store, err := registry.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("registry.Open failed: %v", err)
	}
	t.Cleanup(store.Close)

	if err := store.Delete(ctx, "channel-never-saved"); err != nil {
		t.Fatalf("expected Delete of an unknown channel to be a no-op, got error: %v", err)
	}
}

func TestRegistryStoreForShardsExcludesUnownedShards(t *testing.T) {
	dsn := startPostgresForGateway(t)
	ctx := context.Background()

	store, err := registry.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("registry.Open failed: %v", err)
	}
	t.Cleanup(store.Close)

	if err := store.Save(ctx, "channel-not-owned", "jid@s.whatsapp.net", "tenant-1"); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	sessions, err := store.ForShards(ctx, []int{7}, func(string) int { return 3 })
	if err != nil {
		t.Fatalf("ForShards failed: %v", err)
	}
	if len(sessions) != 0 {
		t.Fatalf("expected no sessions for an unowned shard, got %+v", sessions)
	}
}
