package registry

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/w3nder/whatsmeow-gateway/internal/channelsettings"
)

const createTableSQL = `CREATE TABLE IF NOT EXISTS gateway_channel_sessions (
	channel_id text PRIMARY KEY,
	jid text NOT NULL,
	tenant_id text NOT NULL,
	paired_at timestamptz NOT NULL DEFAULT now()
)`

const addSettingsColumnsSQL = `ALTER TABLE gateway_channel_sessions
	ADD COLUMN IF NOT EXISTS listen_groups boolean NOT NULL DEFAULT true,
	ADD COLUMN IF NOT EXISTS receive_calls boolean NOT NULL DEFAULT true,
	ADD COLUMN IF NOT EXISTS call_reject_message text`

const sessionColumns = `channel_id, jid, tenant_id, listen_groups, receive_calls, call_reject_message`

type ChannelSession struct {
	ChannelID string
	JID       string
	TenantID  string
	Settings  channelsettings.Settings
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanSession(row rowScanner) (ChannelSession, error) {
	var cs ChannelSession
	var message *string
	if err := row.Scan(&cs.ChannelID, &cs.JID, &cs.TenantID, &cs.Settings.ListenGroups, &cs.Settings.ReceiveCalls, &message); err != nil {
		return ChannelSession{}, err
	}
	if message != nil {
		cs.Settings.CallRejectMessage = *message
	}
	return cs, nil
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

	if _, err := pool.Exec(ctx, addSettingsColumnsSQL); err != nil {
		pool.Close()
		return nil, fmt.Errorf("registry: add settings columns to gateway_channel_sessions: %w", err)
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

func (s *Store) SaveSettings(ctx context.Context, channelID, tenantID string, settings channelsettings.Settings) error {
	var message *string
	if settings.CallRejectMessage != "" {
		message = &settings.CallRejectMessage
	}
	if _, err := s.pool.Exec(ctx,
		`UPDATE gateway_channel_sessions SET listen_groups = $3, receive_calls = $4, call_reject_message = $5
		 WHERE channel_id = $1 AND tenant_id = $2`,
		channelID, tenantID, settings.ListenGroups, settings.ReceiveCalls, message,
	); err != nil {
		return fmt.Errorf("registry: save settings %s: %w", channelID, err)
	}
	return nil
}

func (s *Store) Get(ctx context.Context, channelID string) (ChannelSession, bool, error) {
	cs, err := scanSession(s.pool.QueryRow(ctx,
		`SELECT `+sessionColumns+` FROM gateway_channel_sessions WHERE channel_id = $1`,
		channelID,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return ChannelSession{}, false, nil
	}
	if err != nil {
		return ChannelSession{}, false, fmt.Errorf("registry: get %s: %w", channelID, err)
	}
	return cs, true, nil
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

	rows, err := s.pool.Query(ctx, `SELECT `+sessionColumns+` FROM gateway_channel_sessions`)
	if err != nil {
		return nil, fmt.Errorf("registry: list sessions: %w", err)
	}
	defer rows.Close()

	var sessions []ChannelSession
	for rows.Next() {
		cs, err := scanSession(rows)
		if err != nil {
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
