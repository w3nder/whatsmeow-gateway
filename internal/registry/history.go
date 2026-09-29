package registry

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

var ErrNoHistoryImport = errors.New("registry: channel has no such history import")

type HistoryImport struct {
	TenantID string
	ImportID string
	Batches  int
}

func (s *Store) BeginHistoryImport(ctx context.Context, channelID, tenantID, importID string) error {
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO gateway_history_imports (channel_id, tenant_id, import_id) VALUES ($1, $2, $3)
		 ON CONFLICT (channel_id) DO UPDATE SET tenant_id = EXCLUDED.tenant_id, import_id = EXCLUDED.import_id, batches = 0, started_at = now()`,
		channelID, tenantID, importID,
	); err != nil {
		return fmt.Errorf("registry: begin history import %s: %w", channelID, err)
	}
	return nil
}

func (s *Store) ActiveHistoryImport(ctx context.Context, channelID string) (HistoryImport, bool, error) {
	var active HistoryImport
	err := s.pool.QueryRow(ctx,
		`SELECT tenant_id, import_id, batches FROM gateway_history_imports WHERE channel_id = $1`,
		channelID,
	).Scan(&active.TenantID, &active.ImportID, &active.Batches)
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
		`UPDATE gateway_history_imports SET batches = batches + 1 WHERE channel_id = $1 AND import_id = $2 RETURNING batches`,
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

func (s *Store) FinishHistoryImport(ctx context.Context, channelID, importID string) error {
	if _, err := s.pool.Exec(ctx,
		`DELETE FROM gateway_history_imports WHERE channel_id = $1 AND import_id = $2`,
		channelID, importID,
	); err != nil {
		return fmt.Errorf("registry: finish history import %s: %w", channelID, err)
	}
	return nil
}
