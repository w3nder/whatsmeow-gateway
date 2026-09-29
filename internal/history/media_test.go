package history_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"google.golang.org/protobuf/proto"

	"github.com/w3nder/whatsmeow-gateway/internal/history"
)

func image(caption string) *waE2E.Message {
	return &waE2E.Message{ImageMessage: &waE2E.ImageMessage{Mimetype: proto.String("image/jpeg"), Caption: proto.String(caption)}}
}

func imageChunk(now time.Time, count int) *waHistorySync.HistorySync {
	messages := make([]*waHistorySync.HistorySyncMsg, 0, count)
	for n := range count {
		messages = append(messages, webMessage(maria, fmt.Sprintf("3EB0IMG%d", n), false, "Maria", now.Add(-time.Hour), image("foto")))
	}
	return chunkOf(waHistorySync.HistorySync_RECENT, 40, conversation(maria, messages...))
}

func mediaOf(t *testing.T, got history.Translation, index int) map[string]any {
	t.Helper()
	media, _ := decodeMessage(t, got.Chats[0].Messages[index])["media"].(map[string]any)
	return media
}

func TestTranslateStoresMediaThatDownloads(t *testing.T) {
	now := time.Now()

	got := translate(t, newFakeSource(), imageChunk(now, 1), now)

	if media := mediaOf(t, got, 0); media["key"] != "inbound-media/tenant-1/3EB0IMG0" || media["mimeType"] != "image/jpeg" {
		t.Fatalf("a downloaded image carries its s3 key, got %v", media)
	}
}

func TestTranslateKeepsAMessageWhoseMediaFailsWithoutAKey(t *testing.T) {
	now := time.Now()
	source := newFakeSource()
	source.mediaErr = errors.New("gone from the cdn")

	got := translate(t, source, imageChunk(now, 1), now)

	if got.Messages != 1 {
		t.Fatalf("the message is imported even when its media is gone, got %+v", got)
	}
	media := mediaOf(t, got, 0)
	if _, hasKey := media["key"]; hasKey || media["mimeType"] != "image/jpeg" || media["caption"] != "foto" {
		t.Fatalf("a failed media is marked by the missing key, got %v", media)
	}
}

func TestTranslateBoundsConcurrentDownloads(t *testing.T) {
	now := time.Now()
	source := newFakeSource()
	source.hold = 30 * time.Millisecond
	limits := history.DefaultLimits()
	limits.MediaConcurrency = 3

	got, err := history.Translate(context.Background(), translateDeps(source), imageChunk(now, 12), limits, now)
	if err != nil {
		t.Fatalf("Translate: %v", err)
	}

	if got.Messages != 12 {
		t.Fatalf("every image is imported, got %d", got.Messages)
	}
	if peak := source.peak.Load(); peak > 3 || peak < 2 {
		t.Fatalf("downloads must run in parallel but never above the limit of 3, peak %d", peak)
	}
}

func TestTranslateMediaTimeoutKeepsTheMessageWithoutHoldingTheChunk(t *testing.T) {
	now := time.Now()
	source := newFakeSource()
	source.blockMedia = true
	limits := history.DefaultLimits()
	limits.MediaConcurrency = 4
	limits.MediaTimeout = 50 * time.Millisecond

	started := time.Now()
	got, err := history.Translate(context.Background(), translateDeps(source), imageChunk(now, 4), limits, now)
	if err != nil {
		t.Fatalf("Translate: %v", err)
	}

	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("a stuck download must give up at its timeout, took %s", elapsed)
	}
	for n := range 4 {
		if _, hasKey := mediaOf(t, got, n)["key"]; hasKey {
			t.Fatalf("a timed-out media has no key, message %d", n)
		}
	}
}

func TestTranslateMediaBudgetCutsTheWholeChunk(t *testing.T) {
	now := time.Now()
	source := newFakeSource()
	source.blockMedia = true
	limits := history.DefaultLimits()
	limits.MediaConcurrency = 2
	limits.MediaTimeout = 10 * time.Second
	limits.MediaBudget = 100 * time.Millisecond

	started := time.Now()
	got, err := history.Translate(context.Background(), translateDeps(source), imageChunk(now, 6), limits, now)
	if err != nil {
		t.Fatalf("Translate: %v", err)
	}

	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("the chunk budget must cut every pending download, took %s", elapsed)
	}
	if got.Messages != 6 {
		t.Fatalf("every message is kept after the budget, got %d", got.Messages)
	}
}
