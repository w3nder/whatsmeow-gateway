package registry

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

var ErrNoHistoryImport = errors.New("registry: channel has no such history import")

const HistoryQuietPeriod = 30 * time.Minute

var liveHistoryImport = fmt.Sprintf(`(h.finished_at IS NULL OR h.touched_at > now() - interval '%d seconds')`, int(HistoryQuietPeriod/time.Second))

type HistoryImport struct {
	TenantID     string
	ImportID     string
	Batches      int
	SkippedChats int
	Finished     bool
}

func (s *Store) BeginHistoryImport(ctx context.Context, channelID, tenantID, importID string) error {
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO gateway_history_imports (channel_id, tenant_id, import_id) VALUES ($1, $2, $3)
		 ON CONFLICT (channel_id) DO UPDATE SET tenant_id = EXCLUDED.tenant_id, import_id = EXCLUDED.import_id,
		 batches = 0, skipped_chats = '{}', finished_at = NULL, started_at = now(), touched_at = now()`,
		channelID, tenantID, importID,
	); err != nil {
		return fmt.Errorf("registry: begin history import %s: %w", channelID, err)
	}
	return nil
}

func (s *Store) ActiveHistoryImport(ctx context.Context, channelID string) (HistoryImport, bool, error) {
	var active HistoryImport
	err := s.pool.QueryRow(ctx,
		`SELECT tenant_id, import_id, batches, cardinality(skipped_chats), finished_at IS NOT NULL
		 FROM gateway_history_imports h WHERE channel_id = $1 AND `+liveHistoryImport,
		channelID,
	).Scan(&active.TenantID, &active.ImportID, &active.Batches, &active.SkippedChats, &active.Finished)
	if errors.Is(err, pgx.ErrNoRows) {
		return HistoryImport{}, false, nil
	}
	if err != nil {
		return HistoryImport{}, false, fmt.Errorf("registry: active history import %s: %w", channelID, err)
	}
	return active, true, nil
}

func (s *Store) CountHistoryBatch(ctx context.Context, channelID, importID string) (int, error) {
	var total int
	err := s.pool.QueryRow(ctx,
		`UPDATE gateway_history_imports h SET batches = batches + 1, touched_at = now()
		 WHERE channel_id = $1 AND import_id = $2 AND `+liveHistoryImport+` RETURNING batches`,
		channelID, importID,
	).Scan(&total)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("registry: count history batch %s: %w", channelID, ErrNoHistoryImport)
	}
	if err != nil {
		return 0, fmt.Errorf("registry: count history batch %s: %w", channelID, err)
	}
	return total, nil
}

func (s *Store) SkipHistoryChats(ctx context.Context, channelID, importID string, chats []string) (int, error) {
	var total int
	err := s.pool.QueryRow(ctx,
		`UPDATE gateway_history_imports h SET skipped_chats = ARRAY(SELECT DISTINCT unnest(skipped_chats || $3::text[])), touched_at = now()
		 WHERE channel_id = $1 AND import_id = $2 AND `+liveHistoryImport+` RETURNING cardinality(skipped_chats)`,
		channelID, importID, chats,
	).Scan(&total)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("registry: skip history chats %s: %w", channelID, ErrNoHistoryImport)
	}
	if err != nil {
		return 0, fmt.Errorf("registry: skip history chats %s: %w", channelID, err)
	}
	return total, nil
}

func (s *Store) FinishHistoryImport(ctx context.Context, channelID, importID string) error {
	if _, err := s.pool.Exec(ctx,
		`UPDATE gateway_history_imports SET finished_at = now(), touched_at = now()
		 WHERE channel_id = $1 AND import_id = $2 AND finished_at IS NULL`,
		channelID, importID,
	); err != nil {
		return fmt.Errorf("registry: finish history import %s: %w", channelID, err)
	}
	return nil
}

func (s *Store) ClearHistoryImport(ctx context.Context, channelID string) error {
	if _, err := s.pool.Exec(ctx,
		`DELETE FROM gateway_history_imports WHERE channel_id = $1`,
		channelID,
	); err != nil {
		return fmt.Errorf("registry: clear history import %s: %w", channelID, err)
	}
	return nil
}
