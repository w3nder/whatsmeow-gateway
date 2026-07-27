package ownership

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const shardOwnerKeyPrefix = "gateway:shard-owner:"

const claimScript = `
local current = redis.call("GET", KEYS[1])
if current == false then
	redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2])
	return 1
end
if current == ARGV[1] then
	redis.call("PEXPIRE", KEYS[1], ARGV[2])
	return 1
end
return 0
`

const renewScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
	redis.call("PEXPIRE", KEYS[1], ARGV[2])
	return 1
end
return 0
`

const releaseScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0
`

type Store struct {
	client     *redis.Client
	shardCount int
}

func NewStore(client *redis.Client, shardCount int) *Store {
	return &Store{client: client, shardCount: shardCount}
}

func shardOwnerKey(shard int) string {
	return fmt.Sprintf("%s%d", shardOwnerKeyPrefix, shard)
}

func (s *Store) Claim(ctx context.Context, shard int, instanceID string, ttl time.Duration) (bool, error) {
	res, err := s.client.Eval(ctx, claimScript, []string{shardOwnerKey(shard)}, instanceID, ttl.Milliseconds()).Result()
	if err != nil {
		return false, err
	}
	return res.(int64) == 1, nil
}

func (s *Store) Renew(ctx context.Context, shard int, instanceID string, ttl time.Duration) (bool, error) {
	res, err := s.client.Eval(ctx, renewScript, []string{shardOwnerKey(shard)}, instanceID, ttl.Milliseconds()).Result()
	if err != nil {
		return false, err
	}
	return res.(int64) == 1, nil
}

func (s *Store) Release(ctx context.Context, shard int, instanceID string) (bool, error) {
	res, err := s.client.Eval(ctx, releaseScript, []string{shardOwnerKey(shard)}, instanceID).Result()
	if err != nil {
		return false, err
	}
	return res.(int64) == 1, nil
}

func (s *Store) ClaimAll(ctx context.Context, instanceID string, ttl time.Duration) error {
	for shard := 0; shard < s.shardCount; shard++ {
		ok, err := s.Claim(ctx, shard, instanceID, ttl)
		if err != nil {
			return fmt.Errorf("claim shard %d: %w", shard, err)
		}
		if !ok {
			return fmt.Errorf("claim shard %d: already owned by another instance", shard)
		}
	}
	return nil
}

func (s *Store) ReleaseAll(ctx context.Context, instanceID string) error {
	for shard := 0; shard < s.shardCount; shard++ {
		if _, err := s.Release(ctx, shard, instanceID); err != nil {
			return fmt.Errorf("release shard %d: %w", shard, err)
		}
	}
	return nil
}
