package history_test

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waHistorySync"

	"github.com/w3nder/whatsmeow-gateway/internal/history"
)

func newImporterWith(limits history.Limits) (*history.Importer, error) {
	return history.NewImporter(context.Background(), newMemoryStore(&recorder{}), &memoryPublisher{log: &recorder{}}, &memoryMedia{}, limits, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestNewImporterRejectsEveryNonPositiveLimit(t *testing.T) {
	cases := map[string]func(*history.Limits){
		"Window":           func(l *history.Limits) { l.Window = 0 },
		"MediaConcurrency": func(l *history.Limits) { l.MediaConcurrency = 0 },
		"MediaTimeout":     func(l *history.Limits) { l.MediaTimeout = -time.Second },
		"MediaBudget":      func(l *history.Limits) { l.MediaBudget = 0 },
		"MaxMessages":      func(l *history.Limits) { l.MaxMessages = 0 },
		"MaxBytes":         func(l *history.Limits) { l.MaxBytes = -1 },
		"BatchPace":        func(l *history.Limits) { l.BatchPace = -time.Millisecond },
		"PublishTimeout":   func(l *history.Limits) { l.PublishTimeout = 0 },
		"RetryFirst":       func(l *history.Limits) { l.RetryFirst = 0 },
		"RetryMax":         func(l *history.Limits) { l.RetryMax = 0 },
		"Chunks":           func(l *history.Limits) { l.Chunks = 0 },
	}
	for field, breakIt := range cases {
		limits := history.DefaultLimits()
		breakIt(&limits)
		importer, err := newImporterWith(limits)
		if err == nil || importer != nil {
			importer.Close(time.Second)
			t.Fatalf("a %s that is not positive must be refused at construction", field)
		}
		if !strings.Contains(err.Error(), field) {
			t.Fatalf("the error must name %s, got %v", field, err)
		}
	}
}

func TestNewImporterAcceptsTheDefaultsAndAZeroBatchPace(t *testing.T) {
	limits := history.DefaultLimits()
	limits.BatchPace = 0
	importer, err := newImporterWith(limits)
	if err != nil {
		t.Fatalf("the defaults without a pace between batches are valid, got %v", err)
	}
	importer.Close(time.Second)
}

func TestTranslateRefusesAMediaConcurrencyOfZeroInsteadOfBlocking(t *testing.T) {
	now := time.Now()
	limits := history.DefaultLimits()
	limits.MediaConcurrency = 0

	finished := make(chan error, 1)
	go func() {
		_, err := history.Translate(context.Background(), translateDeps(newFakeSource()), textChunk(waHistorySync.HistorySync_RECENT, 40, 2), limits, now)
		finished <- err
	}()

	select {
	case err := <-finished:
		if err == nil || !strings.Contains(err.Error(), "MediaConcurrency") {
			t.Fatalf("a translation without download workers must be refused, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a translation without download workers must fail at once, never block")
	}
}
