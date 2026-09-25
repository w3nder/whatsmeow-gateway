package dedupe

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const webMessageIDPrefix = "3EB0"

const createTableSQL = `CREATE TABLE IF NOT EXISTS gateway_sent_messages (
	message_id text PRIMARY KEY,
	provider_message_id text NOT NULL,
	status text NOT NULL,
	updated_at timestamptz NOT NULL DEFAULT now()
)`

const statusPending = "pending"
const statusSent = "sent"
const statusFailed = "failed"

const createActionsTableSQL = `CREATE TABLE IF NOT EXISTS gateway_group_actions (
	command_id text NOT NULL,
	group_jid text NOT NULL,
	status text NOT NULL,
	removed integer,
	updated_at timestamptz NOT NULL DEFAULT now(),
	PRIMARY KEY (command_id, group_jid)
)`

const addActionFailureColumnSQL = `ALTER TABLE gateway_group_actions ADD COLUMN IF NOT EXISTS failure text`

type Store struct {
	pool *pgxpool.Pool
}

func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("dedupe: connect: %w", err)
	}

	if _, err := pool.Exec(ctx, createTableSQL); err != nil {
		pool.Close()
		return nil, fmt.Errorf("dedupe: create gateway_sent_messages table: %w", err)
	}

	if _, err := pool.Exec(ctx, createActionsTableSQL); err != nil {
		pool.Close()
		return nil, fmt.Errorf("dedupe: create gateway_group_actions table: %w", err)
	}

	if _, err := pool.Exec(ctx, addActionFailureColumnSQL); err != nil {
		pool.Close()
		return nil, fmt.Errorf("dedupe: add failure column to gateway_group_actions: %w", err)
	}

	return &Store{pool: pool}, nil
}

func DeterministicProviderID(messageID string) string {
	hash := sha256.Sum256([]byte(messageID))
	return webMessageIDPrefix + strings.ToUpper(hex.EncodeToString(hash[:9]))
}

func (s *Store) Begin(ctx context.Context, messageID, providerID string) (bool, string, error) {
	var claimed string
	err := s.pool.QueryRow(ctx,
		`INSERT INTO gateway_sent_messages (message_id, provider_message_id, status) VALUES ($1, $2, $3) ON CONFLICT (message_id) DO NOTHING RETURNING message_id`,
		messageID, providerID, statusPending,
	).Scan(&claimed)
	if err == nil {
		return false, providerID, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return false, "", fmt.Errorf("dedupe: begin %s: %w", messageID, err)
	}

	var existingProviderID, status string
	if err := s.pool.QueryRow(ctx,
		`SELECT provider_message_id, status FROM gateway_sent_messages WHERE message_id = $1`,
		messageID,
	).Scan(&existingProviderID, &status); err != nil {
		return false, "", fmt.Errorf("dedupe: read back %s: %w", messageID, err)
	}

	return status == statusSent, existingProviderID, nil
}

func (s *Store) MarkSent(ctx context.Context, messageID string) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE gateway_sent_messages SET status = $2, updated_at = now() WHERE message_id = $1`,
		messageID, statusSent,
	)
	if err != nil {
		return fmt.Errorf("dedupe: mark sent %s: %w", messageID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("dedupe: mark sent %s: no ledger row found", messageID)
	}
	return nil
}

type ActionRecord struct {
	Finished bool
	Removed  *int
	Failure  string
}

func (s *Store) BeginAction(ctx context.Context, commandID, groupJID string) (ActionRecord, error) {
	var status string
	var removed *int
	var failure *string
	err := s.pool.QueryRow(ctx,
		`INSERT INTO gateway_group_actions (command_id, group_jid, status) VALUES ($1, $2, $3)
		 ON CONFLICT (command_id, group_jid) DO UPDATE SET updated_at = now()
		 RETURNING status, removed, failure`,
		commandID, groupJID, statusPending,
	).Scan(&status, &removed, &failure)
	if err != nil {
		return ActionRecord{}, fmt.Errorf("dedupe: begin action %s/%s: %w", commandID, groupJID, err)
	}
	record := ActionRecord{Finished: status == statusSent || status == statusFailed, Removed: removed}
	if status == statusFailed && failure != nil {
		record.Failure = *failure
	}
	return record, nil
}

func (s *Store) MarkActionDone(ctx context.Context, commandID, groupJID string, removed *int) error {
	return s.finishAction(ctx, commandID, groupJID, statusSent, removed, nil)
}

func (s *Store) MarkActionFailed(ctx context.Context, commandID, groupJID, failure string) error {
	return s.finishAction(ctx, commandID, groupJID, statusFailed, nil, &failure)
}

func (s *Store) finishAction(ctx context.Context, commandID, groupJID, status string, removed *int, failure *string) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE gateway_group_actions SET status = $3, removed = $4, failure = $5, updated_at = now() WHERE command_id = $1 AND group_jid = $2`,
		commandID, groupJID, status, removed, failure,
	)
	if err != nil {
		return fmt.Errorf("dedupe: finish action %s/%s as %s: %w", commandID, groupJID, status, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("dedupe: finish action %s/%s as %s: no ledger row found", commandID, groupJID, status)
	}
	return nil
}

func (s *Store) Close() {
	s.pool.Close()
}
