package test

import (
	"context"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/w3nder/whatsmeow-gateway/internal/ownership"
)

func TestShardIsStableAndInRange(t *testing.T) {
	const n = 1024

	for _, channelID := range []string{"channel-1", "channel-2", "another-channel-id", ""} {
		first := ownership.Shard(channelID, n)
		second := ownership.Shard(channelID, n)
		if first != second {
			t.Fatalf("Shard(%q, %d) not stable: got %d then %d", channelID, n, first, second)
		}
		if first < 0 || first >= n {
			t.Fatalf("Shard(%q, %d) = %d, want in [0,%d)", channelID, n, first, n)
		}
	}
}

func startRedis(t *testing.T) *goredis.Client {
	t.Helper()
	ctx := context.Background()

	container, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("failed to start redis container: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Errorf("failed to terminate redis container: %v", err)
		}
	})

	connString, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	opts, err := goredis.ParseURL(connString)
	if err != nil {
		t.Fatalf("failed to parse redis url: %v", err)
	}

	client := goredis.NewClient(opts)
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("failed to close redis client: %v", err)
		}
	})

	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("failed to ping redis: %v", err)
	}

	return client
}

func TestStoreClaimSucceedsOnce(t *testing.T) {
	client := startRedis(t)
	store := ownership.NewStore(client, 1024)
	ctx := context.Background()

	ok, err := store.Claim(ctx, 42, "instance-a", 5*time.Second)
	if err != nil {
		t.Fatalf("Claim failed: %v", err)
	}
	if !ok {
		t.Fatal("expected first Claim to succeed")
	}
}

func TestStoreClaimByDifferentInstanceFailsWhileHeld(t *testing.T) {
	client := startRedis(t)
	store := ownership.NewStore(client, 1024)
	ctx := context.Background()

	ok, err := store.Claim(ctx, 7, "instance-a", 5*time.Second)
	if err != nil {
		t.Fatalf("Claim failed: %v", err)
	}
	if !ok {
		t.Fatal("expected first Claim to succeed")
	}

	ok, err = store.Claim(ctx, 7, "instance-b", 5*time.Second)
	if err != nil {
		t.Fatalf("Claim (other instance) failed: %v", err)
	}
	if ok {
		t.Fatal("expected Claim by a different instance to fail while shard is held")
	}
}

func TestStoreClaimBySameInstanceRenews(t *testing.T) {
	client := startRedis(t)
	store := ownership.NewStore(client, 1024)
	ctx := context.Background()

	ok, err := store.Claim(ctx, 3, "instance-a", 5*time.Second)
	if err != nil {
		t.Fatalf("Claim failed: %v", err)
	}
	if !ok {
		t.Fatal("expected first Claim to succeed")
	}

	ok, err = store.Claim(ctx, 3, "instance-a", 5*time.Second)
	if err != nil {
		t.Fatalf("re-Claim by same instance failed: %v", err)
	}
	if !ok {
		t.Fatal("expected re-Claim by the same instance to succeed (renew)")
	}
}

func TestStoreRenewExtendsOnlyForOwner(t *testing.T) {
	client := startRedis(t)
	store := ownership.NewStore(client, 1024)
	ctx := context.Background()

	if _, err := store.Claim(ctx, 9, "instance-a", 5*time.Second); err != nil {
		t.Fatalf("Claim failed: %v", err)
	}

	ok, err := store.Renew(ctx, 9, "instance-b", 5*time.Second)
	if err != nil {
		t.Fatalf("Renew (non-owner) failed: %v", err)
	}
	if ok {
		t.Fatal("expected Renew by a non-owner to return false")
	}

	ok, err = store.Renew(ctx, 9, "instance-a", 5*time.Second)
	if err != nil {
		t.Fatalf("Renew (owner) failed: %v", err)
	}
	if !ok {
		t.Fatal("expected Renew by the owner to succeed")
	}
}

func TestStoreReleaseByOwnerFreesShard(t *testing.T) {
	client := startRedis(t)
	store := ownership.NewStore(client, 1024)
	ctx := context.Background()

	if _, err := store.Claim(ctx, 11, "instance-a", 5*time.Second); err != nil {
		t.Fatalf("Claim failed: %v", err)
	}

	released, err := store.Release(ctx, 11, "instance-a")
	if err != nil {
		t.Fatalf("Release failed: %v", err)
	}
	if !released {
		t.Fatal("expected Release by the owner to free the shard")
	}

	ok, err := store.Claim(ctx, 11, "instance-b", 5*time.Second)
	if err != nil {
		t.Fatalf("Claim after Release failed: %v", err)
	}
	if !ok {
		t.Fatal("expected shard to be claimable by a different instance after Release")
	}
}

func TestStoreReleaseByNonOwnerDoesNotFreeShard(t *testing.T) {
	client := startRedis(t)
	store := ownership.NewStore(client, 1024)
	ctx := context.Background()

	if _, err := store.Claim(ctx, 13, "instance-a", 5*time.Second); err != nil {
		t.Fatalf("Claim failed: %v", err)
	}

	released, err := store.Release(ctx, 13, "instance-b")
	if err != nil {
		t.Fatalf("Release (non-owner) failed: %v", err)
	}
	if released {
		t.Fatal("expected Release by a non-owner to be a no-op (fencing)")
	}

	ok, err := store.Claim(ctx, 13, "instance-b", 5*time.Second)
	if err != nil {
		t.Fatalf("Claim after non-owner Release failed: %v", err)
	}
	if ok {
		t.Fatal("expected shard to still be held by instance-a after a non-owner Release")
	}
}

func TestStoreClaimAllAndReleaseAll(t *testing.T) {
	client := startRedis(t)
	const shardCount = 4
	store := ownership.NewStore(client, shardCount)
	ctx := context.Background()

	if err := store.ClaimAll(ctx, "instance-a", 5*time.Second); err != nil {
		t.Fatalf("ClaimAll failed: %v", err)
	}

	for shard := 0; shard < shardCount; shard++ {
		ok, err := store.Claim(ctx, shard, "instance-b", 5*time.Second)
		if err != nil {
			t.Fatalf("Claim(shard %d, instance-b) failed: %v", shard, err)
		}
		if ok {
			t.Fatalf("expected shard %d to still be owned by instance-a after ClaimAll", shard)
		}
	}

	if err := store.ReleaseAll(ctx, "instance-a"); err != nil {
		t.Fatalf("ReleaseAll failed: %v", err)
	}

	for shard := 0; shard < shardCount; shard++ {
		ok, err := store.Claim(ctx, shard, "instance-b", 5*time.Second)
		if err != nil {
			t.Fatalf("Claim(shard %d, instance-b) after ReleaseAll failed: %v", shard, err)
		}
		if !ok {
			t.Fatalf("expected shard %d to be claimable by instance-b after ReleaseAll", shard)
		}
	}
}
