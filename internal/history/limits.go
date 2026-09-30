package history

import (
	"errors"
	"fmt"
	"time"

	"go.mau.fi/whatsmeow/proto/waHistorySync"

	"github.com/w3nder/whatsmeow-gateway/internal/deviceprops"
)

type Limits struct {
	Window           time.Duration
	MediaConcurrency int
	MediaTimeout     time.Duration
	MediaBudget      time.Duration
	MaxMessages      int
	MaxBytes         int
	BatchPace        time.Duration
	PublishTimeout   time.Duration
	RetryFirst       time.Duration
	RetryMax         time.Duration
	Chunks           int
}

func DefaultLimits() Limits {
	return Limits{
		Window:           deviceprops.WindowDays * 24 * time.Hour,
		MediaConcurrency: 4,
		MediaTimeout:     time.Minute,
		MediaBudget:      10 * time.Minute,
		MaxMessages:      500,
		MaxBytes:         1_000_000,
		BatchPace:        250 * time.Millisecond,
		PublishTimeout:   10 * time.Second,
		RetryFirst:       time.Second,
		RetryMax:         30 * time.Second,
		Chunks:           2,
	}
}

func (l Limits) validate() error {
	positive := []struct {
		name  string
		value int64
	}{
		{"Window", int64(l.Window)},
		{"MediaConcurrency", int64(l.MediaConcurrency)},
		{"MediaTimeout", int64(l.MediaTimeout)},
		{"MediaBudget", int64(l.MediaBudget)},
		{"MaxMessages", int64(l.MaxMessages)},
		{"MaxBytes", int64(l.MaxBytes)},
		{"PublishTimeout", int64(l.PublishTimeout)},
		{"RetryFirst", int64(l.RetryFirst)},
		{"RetryMax", int64(l.RetryMax)},
		{"Chunks", int64(l.Chunks)},
	}
	var invalid []error
	for _, limit := range positive {
		if limit.value <= 0 {
			invalid = append(invalid, fmt.Errorf("history: limit %s must be positive, got %d", limit.name, limit.value))
		}
	}
	if l.BatchPace < 0 {
		invalid = append(invalid, fmt.Errorf("history: limit BatchPace must not be negative, got %s", l.BatchPace))
	}
	return errors.Join(invalid...)
}

func importable(syncType waHistorySync.HistorySync_HistorySyncType) bool {
	switch syncType {
	case waHistorySync.HistorySync_INITIAL_BOOTSTRAP, waHistorySync.HistorySync_RECENT, waHistorySync.HistorySync_FULL:
		return true
	default:
		return false
	}
}

func finishesImport(data *waHistorySync.HistorySync) bool {
	return importable(data.GetSyncType()) && data.GetProgress() >= 100
}
