package history

import (
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
