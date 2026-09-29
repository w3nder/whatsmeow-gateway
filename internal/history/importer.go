package history

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/mapper"
	"github.com/w3nder/whatsmeow-gateway/internal/registry"
)

type Source interface {
	MessageSource
	DownloadHistory(ctx context.Context, notif *waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error)
	ReleaseHistory(ctx context.Context, notif *waE2E.HistorySyncNotification) error
}

type Store interface {
	ActiveHistoryImport(ctx context.Context, channelID string) (registry.HistoryImport, bool, error)
	CountHistoryBatch(ctx context.Context, channelID, importID string) (int, error)
	FinishHistoryImport(ctx context.Context, channelID, importID string) error
}

type Publisher interface {
	PublishHistoryBatch(ctx context.Context, batch amqp.HistoryBatch) error
	PublishHistoryDone(ctx context.Context, done amqp.HistoryDone) error
}

type Importer struct {
	store     Store
	publisher Publisher
	media     mapper.MediaStore
	limits    Limits
	logger    *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	chunks chan struct{}

	mu      sync.Mutex
	closed  bool
	lanes   map[string]*sync.Mutex
	running sync.WaitGroup
}

func NewImporter(ctx context.Context, store Store, publisher Publisher, media mapper.MediaStore, limits Limits, logger *slog.Logger) *Importer {
	ctx, cancel := context.WithCancel(ctx)
	return &Importer{
		store:     store,
		publisher: publisher,
		media:     media,
		limits:    limits,
		logger:    logger,
		ctx:       ctx,
		cancel:    cancel,
		chunks:    make(chan struct{}, limits.Chunks),
		lanes:     make(map[string]*sync.Mutex),
	}
}

func (i *Importer) Accept(channelID string, source Source, notif *waE2E.HistorySyncNotification) {
	lane, ok := i.enter(channelID)
	if !ok {
		i.logger.Warn("history: chunk arrived while the importer is closing, dropped", "channel_id", channelID)
		return
	}
	go func() {
		defer i.running.Done()
		lane.Lock()
		defer lane.Unlock()
		select {
		case i.chunks <- struct{}{}:
		case <-i.ctx.Done():
			return
		}
		defer func() { <-i.chunks }()
		if err := i.Import(i.ctx, channelID, source, notif); err != nil {
			i.logger.Error("history: import chunk", "channel_id", channelID, "error", err)
		}
	}()
}

func (i *Importer) enter(channelID string) (*sync.Mutex, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed {
		return nil, false
	}
	i.running.Add(1)
	lane, ok := i.lanes[channelID]
	if !ok {
		lane = &sync.Mutex{}
		i.lanes[channelID] = lane
	}
	return lane, true
}

func (i *Importer) Close(timeout time.Duration) {
	i.mu.Lock()
	i.closed = true
	i.mu.Unlock()

	done := make(chan struct{})
	go func() {
		i.running.Wait()
		close(done)
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		i.logger.Error("history: chunks still importing at shutdown, cancelling them", "timeout", timeout)
	}
	i.cancel()
	<-done
}

func (i *Importer) Import(ctx context.Context, channelID string, source Source, notif *waE2E.HistorySyncNotification) error {
	data, err := source.DownloadHistory(ctx, notif)
	if err != nil {
		return fmt.Errorf("history: download chunk of %s: %w", channelID, err)
	}
	if importable(data.GetSyncType()) {
		if err := i.importChunk(ctx, channelID, source, data); err != nil {
			return err
		}
	}
	if err := source.ReleaseHistory(ctx, notif); err != nil {
		return fmt.Errorf("history: release chunk of %s: %w", channelID, err)
	}
	return nil
}

func (i *Importer) importChunk(ctx context.Context, channelID string, source Source, data *waHistorySync.HistorySync) error {
	active, found, err := i.store.ActiveHistoryImport(ctx, channelID)
	if err != nil {
		return fmt.Errorf("history: look up the import of %s: %w", channelID, err)
	}
	if !found {
		i.logger.Info("history: chunk without an active import, discarded",
			"channel_id", channelID,
			"sync_type", data.GetSyncType().String(),
			"chunk_order", data.GetChunkOrder())
		return nil
	}
	err = i.publishChunk(ctx, channelID, active, source, data)
	if errors.Is(err, registry.ErrNoHistoryImport) {
		i.logger.Warn("history: import replaced while its chunk was publishing", "channel_id", channelID, "import_id", active.ImportID)
		return nil
	}
	return err
}

func (i *Importer) publishChunk(ctx context.Context, channelID string, active registry.HistoryImport, source Source, data *waHistorySync.HistorySync) error {
	translation, err := Translate(ctx, TranslateDeps{Source: source, Media: i.media, ChannelID: channelID, TenantID: active.TenantID}, data, i.limits, time.Now())
	if err != nil {
		return err
	}
	batches := Split(translation.Chats, i.limits.MaxMessages, i.limits.MaxBytes)
	total := active.Batches
	for n, chats := range batches {
		if n > 0 {
			if err := sleep(ctx, i.limits.BatchPace); err != nil {
				return err
			}
		}
		batch := amqp.HistoryBatch{
			TenantID:       active.TenantID,
			ChannelID:      channelID,
			ImportID:       active.ImportID,
			ChunkOrder:     data.GetChunkOrder(),
			SourceProgress: data.GetProgress(),
			BatchIndex:     n + 1,
			BatchesInChunk: len(batches),
			Chats:          chats,
		}
		if err := i.retry(ctx, func(ctx context.Context) error {
			if err := i.stillActive(ctx, channelID, active.ImportID); err != nil {
				return err
			}
			return i.publisher.PublishHistoryBatch(ctx, batch)
		}); err != nil {
			return err
		}
		if total, err = i.store.CountHistoryBatch(ctx, channelID, active.ImportID); err != nil {
			return err
		}
	}

	i.logger.Info("history: chunk published",
		"channel_id", channelID,
		"import_id", active.ImportID,
		"sync_type", data.GetSyncType().String(),
		"chunk_order", data.GetChunkOrder(),
		"progress", data.GetProgress(),
		"batches", len(batches),
		"messages", translation.Messages,
		"out_of_window", translation.OutOfWindow,
		"skipped", translation.Skipped,
		"chats_without_phone", translation.ChatsWithoutPhone)

	if !finishesImport(data) {
		return nil
	}
	done := amqp.HistoryDone{TenantID: active.TenantID, ChannelID: channelID, ImportID: active.ImportID, TotalBatches: total}
	if err := i.retry(ctx, func(ctx context.Context) error {
		if err := i.stillActive(ctx, channelID, active.ImportID); err != nil {
			return err
		}
		return i.publisher.PublishHistoryDone(ctx, done)
	}); err != nil {
		return err
	}
	return i.store.FinishHistoryImport(ctx, channelID, active.ImportID)
}

func (i *Importer) stillActive(ctx context.Context, channelID, importID string) error {
	active, found, err := i.store.ActiveHistoryImport(ctx, channelID)
	if err != nil {
		return fmt.Errorf("history: look up the import of %s: %w", channelID, err)
	}
	if !found || active.ImportID != importID {
		return fmt.Errorf("history: import %s of %s: %w", importID, channelID, registry.ErrNoHistoryImport)
	}
	return nil
}

func (i *Importer) retry(ctx context.Context, publish func(context.Context) error) error {
	delay := i.limits.RetryFirst
	for {
		attempt, cancel := context.WithTimeout(ctx, i.limits.PublishTimeout)
		err := publish(attempt)
		cancel()
		if err == nil || errors.Is(err, registry.ErrNoHistoryImport) {
			return err
		}
		i.logger.Warn("history: publish not confirmed, retrying", "error", err, "retry_in", delay)
		if err := sleep(ctx, delay); err != nil {
			return err
		}
		delay = min(delay*2, i.limits.RetryMax)
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
