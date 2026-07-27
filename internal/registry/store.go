package registry

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

const createTableSQL = `CREATE TABLE IF NOT EXISTS gateway_channel_sessions (
	channel_id text PRIMARY KEY,
	jid text NOT NULL,
	tenant_id text NOT NULL,
	paired_at timestamptz NOT NULL DEFAULT now()
)`

type ChannelSession struct {
	ChannelID string
	JID       string
	TenantID  string
}

type Store struct {
	pool *pgxpool.Pool
}

func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("registry: connect: %w", err)
	}

	if _, err := pool.Exec(ctx, createTableSQL); err != nil {
		pool.Close()
		return nil, fmt.Errorf("registry: create gateway_channel_sessions table: %w", err)
	}

	return &Store{pool: pool}, nil
}

func (s *Store) Save(ctx context.Context, channelID, jid, tenantID string) error {
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO gateway_channel_sessions (channel_id, jid, tenant_id) VALUES ($1, $2, $3)
		 ON CONFLICT (channel_id) DO UPDATE SET jid = EXCLUDED.jid, tenant_id = EXCLUDED.tenant_id, paired_at = now()`,
		channelID, jid, tenantID,
	); err != nil {
		return fmt.Errorf("registry: save %s: %w", channelID, err)
	}
	return nil
}

func (s *Store) Delete(ctx context.Context, channelID string) error {
	if _, err := s.pool.Exec(ctx, `DELETE FROM gateway_channel_sessions WHERE channel_id = $1`, channelID); err != nil {
		return fmt.Errorf("registry: delete %s: %w", channelID, err)
	}
	return nil
}

func (s *Store) ForShards(ctx context.Context, shards []int, shardFn func(channelID string) int) ([]ChannelSession, error) {
	owned := make(map[int]struct{}, len(shards))
	for _, shard := range shards {
		owned[shard] = struct{}{}
	}

	rows, err := s.pool.Query(ctx, `SELECT channel_id, jid, tenant_id FROM gateway_channel_sessions`)
	if err != nil {
		return nil, fmt.Errorf("registry: list sessions: %w", err)
	}
	defer rows.Close()

	var sessions []ChannelSession
	for rows.Next() {
		var cs ChannelSession
		if err := rows.Scan(&cs.ChannelID, &cs.JID, &cs.TenantID); err != nil {
			return nil, fmt.Errorf("registry: scan session row: %w", err)
		}
		if _, ok := owned[shardFn(cs.ChannelID)]; ok {
			sessions = append(sessions, cs)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("registry: iterate sessions: %w", err)
	}

	return sessions, nil
}

func (s *Store) Close() {
	s.pool.Close()
}
