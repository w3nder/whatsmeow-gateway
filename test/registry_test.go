package test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/w3nder/whatsmeow-gateway/internal/channelsettings"
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

func TestRegistryStoreGetReturnsTheDefaultSettingsForAFreshSession(t *testing.T) {
	dsn := startPostgresForGateway(t)
	ctx := context.Background()

	store, err := registry.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("registry.Open failed: %v", err)
	}
	t.Cleanup(store.Close)

	if err := store.Save(ctx, "channel-defaults", "jid@s.whatsapp.net", "tenant-1"); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	got, found, err := store.Get(ctx, "channel-defaults")
	if err != nil || !found {
		t.Fatalf("Get = found %v, err %v", found, err)
	}
	if got.Settings != channelsettings.Defaults() {
		t.Fatalf("a fresh session must carry the defaults, got %+v", got.Settings)
	}
}

func TestRegistryStoreSaveSettingsRoundTripsThroughGetAndForShards(t *testing.T) {
	dsn := startPostgresForGateway(t)
	ctx := context.Background()

	store, err := registry.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("registry.Open failed: %v", err)
	}
	t.Cleanup(store.Close)

	if err := store.Save(ctx, "channel-1", "jid@s.whatsapp.net", "tenant-1"); err != nil {
		t.Fatalf("Save failed: %v", err)
	}
	want := channelsettings.Settings{ListenGroups: false, ReceiveCalls: false, CallRejectMessage: "Não atendemos ligações.", Version: 6}
	if err := store.SaveSettings(ctx, "channel-1", "tenant-1", want); err != nil {
		t.Fatalf("SaveSettings failed: %v", err)
	}

	got, _, err := store.Get(ctx, "channel-1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got.Settings != want {
		t.Fatalf("Get settings = %+v, want %+v", got.Settings, want)
	}

	sessions, err := store.ForShards(ctx, []int{0}, func(string) int { return 0 })
	if err != nil {
		t.Fatalf("ForShards failed: %v", err)
	}
	if len(sessions) != 1 || sessions[0].Settings != want {
		t.Fatalf("ForShards settings = %+v, want %+v", sessions, want)
	}
}

func TestRegistryStoreSaveSettingsWithAnEmptyMessageClearsIt(t *testing.T) {
	dsn := startPostgresForGateway(t)
	ctx := context.Background()

	store, err := registry.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("registry.Open failed: %v", err)
	}
	t.Cleanup(store.Close)

	if err := store.Save(ctx, "channel-1", "jid@s.whatsapp.net", "tenant-1"); err != nil {
		t.Fatalf("Save failed: %v", err)
	}
	withMessage := channelsettings.Settings{ListenGroups: true, ReceiveCalls: false, CallRejectMessage: "texto"}
	if err := store.SaveSettings(ctx, "channel-1", "tenant-1", withMessage); err != nil {
		t.Fatalf("SaveSettings(with message) failed: %v", err)
	}
	cleared := channelsettings.Settings{ListenGroups: true, ReceiveCalls: false}
	if err := store.SaveSettings(ctx, "channel-1", "tenant-1", cleared); err != nil {
		t.Fatalf("SaveSettings(cleared) failed: %v", err)
	}

	got, _, err := store.Get(ctx, "channel-1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got.Settings != cleared {
		t.Fatalf("settings = %+v, want the message cleared %+v", got.Settings, cleared)
	}
}

func TestRegistryStoreSaveSettingsIgnoresAnotherTenantAndAnUnknownChannel(t *testing.T) {
	dsn := startPostgresForGateway(t)
	ctx := context.Background()

	store, err := registry.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("registry.Open failed: %v", err)
	}
	t.Cleanup(store.Close)

	if err := store.Save(ctx, "channel-1", "jid@s.whatsapp.net", "tenant-1"); err != nil {
		t.Fatalf("Save failed: %v", err)
	}
	muted := channelsettings.Settings{ListenGroups: false, ReceiveCalls: false}

	if err := store.SaveSettings(ctx, "channel-1", "tenant-other", muted); err != nil {
		t.Fatalf("SaveSettings with another tenant failed: %v", err)
	}
	got, _, err := store.Get(ctx, "channel-1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got.Settings != channelsettings.Defaults() {
		t.Fatalf("another tenant must not change the settings, got %+v", got.Settings)
	}

	if err := store.SaveSettings(ctx, "channel-unknown", "tenant-1", muted); err != nil {
		t.Fatalf("SaveSettings for an unknown channel failed: %v", err)
	}
	if _, found, err := store.Get(ctx, "channel-unknown"); err != nil || found {
		t.Fatalf("SaveSettings must not create a session, found %v err %v", found, err)
	}
}

func TestRegistryOpenAddsTheSettingsColumnsToAnExistingTableKeepingTheDefaults(t *testing.T) {
	dsn := startPostgresForGateway(t)
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New failed: %v", err)
	}
	t.Cleanup(pool.Close)

	if _, err := pool.Exec(ctx, `CREATE TABLE gateway_channel_sessions (
		channel_id text PRIMARY KEY,
		jid text NOT NULL,
		tenant_id text NOT NULL,
		paired_at timestamptz NOT NULL DEFAULT now()
	)`); err != nil {
		t.Fatalf("create legacy table failed: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO gateway_channel_sessions (channel_id, jid, tenant_id) VALUES ('legacy', 'jid@s.whatsapp.net', 'tenant-1')`); err != nil {
		t.Fatalf("insert legacy row failed: %v", err)
	}

	store, err := registry.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("registry.Open on the legacy table failed: %v", err)
	}
	t.Cleanup(store.Close)

	got, found, err := store.Get(ctx, "legacy")
	if err != nil || !found {
		t.Fatalf("Get legacy = found %v, err %v", found, err)
	}
	if got.Settings != channelsettings.Defaults() {
		t.Fatalf("an existing row must take the defaults, got %+v", got.Settings)
	}
}
