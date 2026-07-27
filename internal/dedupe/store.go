package dedupe

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

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

	return &Store{pool: pool}, nil
}

func DeterministicProviderID(messageID string) string {
	hash := sha256.Sum256([]byte(messageID))
	return webMessageIDPrefix + strings.ToUpper(hex.EncodeToString(hash[:9]))
}

func (s *Store) Begin(ctx context.Context, messageID, providerID string) (bool, string, error) {
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO gateway_sent_messages (message_id, provider_message_id, status) VALUES ($1, $2, $3) ON CONFLICT (message_id) DO NOTHING`,
		messageID, providerID, statusPending,
	); err != nil {
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

func (s *Store) Close() {
	s.pool.Close()
}
