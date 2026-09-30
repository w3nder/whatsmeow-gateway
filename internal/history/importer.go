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
	SkipHistoryChats(ctx context.Context, channelID, importID string, chats []string) (int, error)
	FinishHistoryImport(ctx context.Context, channelID, importID string) error
}

type Publisher interface {
	PublishHistoryBatch(ctx context.Context, batch amqp.HistoryBatch) error
	PublishHistoryDone(ctx context.Context, done amqp.HistoryDone) error
}

const storeAttempts = 5

type chunk struct {
	source Source
	notif  *waE2E.HistorySyncNotification
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
	queues  map[string][]chunk
	running sync.WaitGroup
}

func NewImporter(ctx context.Context, store Store, publisher Publisher, media mapper.MediaStore, limits Limits, logger *slog.Logger) (*Importer, error) {
	if err := limits.validate(); err != nil {
		return nil, err
	}
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
		queues:    make(map[string][]chunk),
	}, nil
}

func (i *Importer) Accept(channelID string, source Source, notif *waE2E.HistorySyncNotification) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed {
		i.logger.Warn("history: chunk arrived while the importer is closing, dropped", "channel_id", channelID)
		return
	}
	pending, draining := i.queues[channelID]
	i.queues[channelID] = append(pending, chunk{source: source, notif: notif})
	if draining {
		return
	}
	i.running.Add(1)
	go i.drain(channelID)
}

func (i *Importer) drain(channelID string) {
	defer i.running.Done()
	for {
		next, ok := i.next(channelID)
		if !ok {
			return
		}
		i.importQueued(channelID, next)
	}
}

func (i *Importer) next(channelID string) (chunk, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	pending := i.queues[channelID]
	if len(pending) == 0 {
		delete(i.queues, channelID)
		return chunk{}, false
	}
	i.queues[channelID] = pending[1:]
	return pending[0], true
}

func (i *Importer) importQueued(channelID string, queued chunk) {
	if i.ctx.Err() != nil {
		return
	}
	select {
	case i.chunks <- struct{}{}:
	case <-i.ctx.Done():
		return
	}
	defer func() { <-i.chunks }()
	if err := i.Import(i.ctx, channelID, queued.source, queued.notif); err != nil {
		i.logger.Error("history: import chunk", "channel_id", channelID, "error", err)
	}
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
	skipped, err := i.skipChats(ctx, channelID, active, translation.ChatsWithoutPhone)
	if err != nil {
		return err
	}
	batches, oversized := Split(translation.Chats, i.limits.MaxMessages, i.limits.MaxBytes)
	for _, id := range oversized {
		i.logger.Warn("history: message larger than a batch, dropped", "channel_id", channelID, "import_id", active.ImportID, "provider_message_id", id)
	}
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
		if total, err = i.countBatch(ctx, channelID, active.ImportID, total); err != nil {
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
		"oversized", len(oversized),
		"chats_without_phone", len(translation.ChatsWithoutPhone),
		"after_the_end", active.Finished)

	if active.Finished || !finishesImport(data) {
		return nil
	}
	done := amqp.HistoryDone{TenantID: active.TenantID, ChannelID: channelID, ImportID: active.ImportID, TotalBatches: total, SkippedChats: skipped}
	if err := i.retry(ctx, func(ctx context.Context) error {
		if err := i.stillActive(ctx, channelID, active.ImportID); err != nil {
			return err
		}
		return i.publisher.PublishHistoryDone(ctx, done)
	}); err != nil {
		return err
	}
	return i.persist(ctx, "finish the import", channelID, func(ctx context.Context) error {
		return i.store.FinishHistoryImport(ctx, channelID, active.ImportID)
	})
}

func (i *Importer) skipChats(ctx context.Context, channelID string, active registry.HistoryImport, chats []string) (int, error) {
	skipped := active.SkippedChats
	if len(chats) == 0 {
		return skipped, nil
	}
	err := i.persist(ctx, "record the chats without a phone", channelID, func(ctx context.Context) error {
		recorded, err := i.store.SkipHistoryChats(ctx, channelID, active.ImportID, chats)
		if err == nil {
			skipped = recorded
		}
		return err
	})
	return skipped, err
}

func (i *Importer) countBatch(ctx context.Context, channelID, importID string, published int) (int, error) {
	total := published + 1
	err := i.persist(ctx, "count a published batch", channelID, func(ctx context.Context) error {
		counted, err := i.store.CountHistoryBatch(ctx, channelID, importID)
		if err == nil {
			total = counted
		}
		return err
	})
	return total, err
}

func (i *Importer) persist(ctx context.Context, what, channelID string, write func(context.Context) error) error {
	delay := i.limits.RetryFirst
	var err error
	for attempt := 1; ; attempt++ {
		err = write(ctx)
		if err == nil || errors.Is(err, registry.ErrNoHistoryImport) {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt == storeAttempts {
			break
		}
		i.logger.Warn("history: registry write failed, retrying", "channel_id", channelID, "write", what, "error", err, "retry_in", delay)
		if err := sleep(ctx, delay); err != nil {
			return err
		}
		delay = min(delay*2, i.limits.RetryMax)
	}
	i.logger.Error("history: registry write failed, publishing on without it", "channel_id", channelID, "write", what, "attempts", storeAttempts, "error", err)
	return nil
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
