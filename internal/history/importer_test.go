package history_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/history"
	"github.com/w3nder/whatsmeow-gateway/internal/registry"
)

type recorder struct {
	mu    sync.Mutex
	steps []string
}

func (r *recorder) add(step string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.steps = append(r.steps, step)
}

func (r *recorder) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string{}, r.steps...)
}

func (r *recorder) count(step string) int {
	n := 0
	for _, s := range r.all() {
		if s == step {
			n++
		}
	}
	return n
}

type memoryStore struct {
	log               *recorder
	mu                sync.Mutex
	imports           map[string]registry.HistoryImport
	countErr          error
	replaceAfterCount *registry.HistoryImport
}

func newMemoryStore(log *recorder) *memoryStore {
	return &memoryStore{log: log, imports: map[string]registry.HistoryImport{}}
}

func (s *memoryStore) begin(channelID string, active registry.HistoryImport) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.imports[channelID] = active
}

func (s *memoryStore) ActiveHistoryImport(_ context.Context, channelID string) (registry.HistoryImport, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	active, found := s.imports[channelID]
	return active, found, nil
}

func (s *memoryStore) CountHistoryBatch(_ context.Context, channelID, importID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.countErr != nil {
		return 0, s.countErr
	}
	active, found := s.imports[channelID]
	if !found || active.ImportID != importID {
		return 0, registry.ErrNoHistoryImport
	}
	active.Batches++
	s.imports[channelID] = active
	s.log.add("count")
	if s.replaceAfterCount != nil {
		s.imports[channelID] = *s.replaceAfterCount
		s.replaceAfterCount = nil
	}
	return active.Batches, nil
}

func (s *memoryStore) FinishHistoryImport(_ context.Context, channelID, importID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.imports[channelID].ImportID == importID {
		delete(s.imports, channelID)
	}
	s.log.add("finish")
	return nil
}

type memoryPublisher struct {
	log      *recorder
	hang     bool
	mu       sync.Mutex
	refusals int
	batches  []amqp.HistoryBatch
	dones    []amqp.HistoryDone
}

func (p *memoryPublisher) PublishHistoryBatch(ctx context.Context, batch amqp.HistoryBatch) error {
	if p.hang {
		<-ctx.Done()
		return ctx.Err()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.refusals > 0 {
		p.refusals--
		p.log.add("batch refused")
		return errors.New("broker refused the batch")
	}
	p.batches = append(p.batches, batch)
	p.log.add(fmt.Sprintf("batch %d/%d", batch.BatchIndex, batch.BatchesInChunk))
	return nil
}

func (p *memoryPublisher) PublishHistoryDone(_ context.Context, done amqp.HistoryDone) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dones = append(p.dones, done)
	p.log.add("done")
	return nil
}

func (p *memoryPublisher) published() ([]amqp.HistoryBatch, []amqp.HistoryDone) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]amqp.HistoryBatch{}, p.batches...), append([]amqp.HistoryDone{}, p.dones...)
}

type chunkSource struct {
	*fakeSource
	log          *recorder
	data         *waHistorySync.HistorySync
	downloadHold time.Duration
	downloading  atomic.Int32
	downloadPeak atomic.Int32
}

func (s *chunkSource) DownloadHistory(context.Context, *waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error) {
	raisePeak(&s.downloadPeak, s.downloading.Add(1))
	defer s.downloading.Add(-1)
	s.log.add("download")
	if s.downloadHold > 0 {
		time.Sleep(s.downloadHold)
	}
	return s.data, nil
}

func (s *chunkSource) ReleaseHistory(context.Context, *waE2E.HistorySyncNotification) error {
	s.log.add("release")
	return nil
}

func textChunk(syncType waHistorySync.HistorySync_HistorySyncType, progress uint32, count int) *waHistorySync.HistorySync {
	now := time.Now()
	messages := make([]*waHistorySync.HistorySyncMsg, 0, count)
	for n := range count {
		messages = append(messages, webMessage(maria, fmt.Sprintf("3EB0T%d", n), false, "Maria", now.Add(-time.Hour), text("oi")))
	}
	return chunkOf(syncType, progress, conversation(maria, messages...))
}

func testLimits() history.Limits {
	limits := history.DefaultLimits()
	limits.BatchPace = 0
	limits.PublishTimeout = 50 * time.Millisecond
	limits.RetryFirst = time.Millisecond
	limits.RetryMax = time.Millisecond
	return limits
}

type importerSetup struct {
	log       *recorder
	store     *memoryStore
	publisher *memoryPublisher
	source    *chunkSource
	importer  *history.Importer
}

func newImporterSetup(t *testing.T, data *waHistorySync.HistorySync, limits history.Limits) importerSetup {
	t.Helper()
	log := &recorder{}
	setup := importerSetup{
		log:       log,
		store:     newMemoryStore(log),
		publisher: &memoryPublisher{log: log},
		source:    &chunkSource{fakeSource: newFakeSource(), log: log, data: data},
	}
	setup.importer = history.NewImporter(context.Background(), setup.store, setup.publisher, &memoryMedia{}, limits, slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() { setup.importer.Close(time.Second) })
	return setup
}

func (s importerSetup) importChunk(t *testing.T) error {
	t.Helper()
	return s.importer.Import(context.Background(), "channel-1", s.source, &waE2E.HistorySyncNotification{})
}

var activeImport = registry.HistoryImport{TenantID: "tenant-1", ImportID: "import-1"}

func TestImportPublishesEveryBatchBeforeReleasingTheChunk(t *testing.T) {
	limits := testLimits()
	limits.MaxMessages = 2
	s := newImporterSetup(t, textChunk(waHistorySync.HistorySync_RECENT, 40, 3), limits)
	s.store.begin("channel-1", activeImport)

	if err := s.importChunk(t); err != nil {
		t.Fatalf("Import: %v", err)
	}

	want := []string{"download", "batch 1/2", "count", "batch 2/2", "count", "release"}
	if got := s.log.all(); !reflect.DeepEqual(got, want) {
		t.Fatalf("steps %v, want %v", got, want)
	}
	batches, dones := s.publisher.published()
	first := batches[0]
	if first.TenantID != "tenant-1" || first.ChannelID != "channel-1" || first.ImportID != "import-1" || first.ChunkOrder != 3 || first.SourceProgress != 40 || first.BatchesInChunk != 2 {
		t.Fatalf("batch header wrong: %+v", first)
	}
	if len(dones) != 0 {
		t.Fatalf("a chunk below 100 never ends the import, got %v", dones)
	}
}

func TestImportEndsTheImportWithTheRegistryTotalAfterTheFinalChunk(t *testing.T) {
	s := newImporterSetup(t, textChunk(waHistorySync.HistorySync_RECENT, 100, 1), testLimits())
	s.store.begin("channel-1", registry.HistoryImport{TenantID: "tenant-1", ImportID: "import-1", Batches: 5})

	if err := s.importChunk(t); err != nil {
		t.Fatalf("Import: %v", err)
	}

	want := []string{"download", "batch 1/1", "count", "done", "finish", "release"}
	if got := s.log.all(); !reflect.DeepEqual(got, want) {
		t.Fatalf("steps %v, want %v", got, want)
	}
	_, dones := s.publisher.published()
	if len(dones) != 1 || dones[0].TotalBatches != 6 || dones[0].ImportID != "import-1" || dones[0].TenantID != "tenant-1" {
		t.Fatalf("done must carry the total of the whole import, got %+v", dones)
	}
	if _, found, _ := s.store.ActiveHistoryImport(context.Background(), "channel-1"); found {
		t.Fatal("the import must be finished after the done")
	}
}

func TestImportOfAFinalChunkWithNothingToImportStillEndsTheImport(t *testing.T) {
	data := chunkOf(waHistorySync.HistorySync_RECENT, 100, conversation("120363000000000000@g.us",
		webMessage("120363000000000000@g.us", "3EB0G", false, "Ana", time.Now().Add(-time.Hour), text("grupo")),
	))
	s := newImporterSetup(t, data, testLimits())
	s.store.begin("channel-1", registry.HistoryImport{TenantID: "tenant-1", ImportID: "import-1", Batches: 3})

	if err := s.importChunk(t); err != nil {
		t.Fatalf("Import: %v", err)
	}

	want := []string{"download", "done", "finish", "release"}
	if got := s.log.all(); !reflect.DeepEqual(got, want) {
		t.Fatalf("steps %v, want %v", got, want)
	}
	if _, dones := s.publisher.published(); dones[0].TotalBatches != 3 {
		t.Fatalf("done must carry the batches already published, got %+v", dones)
	}
}

func TestImportWithoutAnActiveImportOnlyDownloadsAndReleases(t *testing.T) {
	s := newImporterSetup(t, textChunk(waHistorySync.HistorySync_RECENT, 100, 2), testLimits())

	if err := s.importChunk(t); err != nil {
		t.Fatalf("Import: %v", err)
	}

	if got := s.log.all(); !reflect.DeepEqual(got, []string{"download", "release"}) {
		t.Fatalf("a chunk without an import is downloaded for its side effects and released, got %v", got)
	}
}

func TestImportOfASyncWithoutConversationsOnlyDownloadsAndReleases(t *testing.T) {
	s := newImporterSetup(t, textChunk(waHistorySync.HistorySync_PUSH_NAME, 100, 2), testLimits())
	s.store.begin("channel-1", activeImport)

	if err := s.importChunk(t); err != nil {
		t.Fatalf("Import: %v", err)
	}

	if got := s.log.all(); !reflect.DeepEqual(got, []string{"download", "release"}) {
		t.Fatalf("a push-name sync is never imported, got %v", got)
	}
	if _, found, _ := s.store.ActiveHistoryImport(context.Background(), "channel-1"); !found {
		t.Fatal("a push-name sync must not end the import")
	}
}

func TestImportRetriesARefusedBatchAndReleasesOnlyAfterTheConfirm(t *testing.T) {
	s := newImporterSetup(t, textChunk(waHistorySync.HistorySync_RECENT, 40, 1), testLimits())
	s.store.begin("channel-1", activeImport)
	s.publisher.refusals = 2

	if err := s.importChunk(t); err != nil {
		t.Fatalf("Import: %v", err)
	}

	want := []string{"download", "batch refused", "batch refused", "batch 1/1", "count", "release"}
	if got := s.log.all(); !reflect.DeepEqual(got, want) {
		t.Fatalf("steps %v, want %v", got, want)
	}
}

func TestImportNeverReleasesAChunkWhoseBatchIsNeverConfirmed(t *testing.T) {
	s := newImporterSetup(t, textChunk(waHistorySync.HistorySync_RECENT, 40, 1), testLimits())
	s.store.begin("channel-1", activeImport)
	s.publisher.hang = true
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	err := s.importer.Import(ctx, "channel-1", s.source, &waE2E.HistorySyncNotification{})

	if err == nil {
		t.Fatal("an unconfirmed batch must fail the chunk")
	}
	if s.log.count("release") != 0 || s.log.count("count") != 0 {
		t.Fatalf("an unconfirmed chunk is never counted nor released, got %v", s.log.all())
	}
}

func TestImportStopsAndReleasesWhenTheImportWasReplaced(t *testing.T) {
	limits := testLimits()
	limits.MaxMessages = 2
	s := newImporterSetup(t, textChunk(waHistorySync.HistorySync_RECENT, 40, 3), limits)
	s.store.begin("channel-1", activeImport)
	s.store.countErr = fmt.Errorf("registry: count history batch channel-1: %w", registry.ErrNoHistoryImport)

	if err := s.importChunk(t); err != nil {
		t.Fatalf("a replaced import is not an error, got %v", err)
	}

	want := []string{"download", "batch 1/2", "release"}
	if got := s.log.all(); !reflect.DeepEqual(got, want) {
		t.Fatalf("steps %v, want %v", got, want)
	}
}

func TestImportPublishesNothingMoreForAnImportReplacedBetweenBatches(t *testing.T) {
	limits := testLimits()
	limits.MaxMessages = 2
	s := newImporterSetup(t, textChunk(waHistorySync.HistorySync_RECENT, 40, 3), limits)
	s.store.begin("channel-1", activeImport)
	s.store.replaceAfterCount = &registry.HistoryImport{TenantID: "tenant-1", ImportID: "import-2"}

	if err := s.importChunk(t); err != nil {
		t.Fatalf("a replaced import is not an error, got %v", err)
	}

	want := []string{"download", "batch 1/2", "count", "release"}
	if got := s.log.all(); !reflect.DeepEqual(got, want) {
		t.Fatalf("steps %v, want %v", got, want)
	}
	batches, _ := s.publisher.published()
	if len(batches) != 1 || batches[0].ImportID != "import-1" {
		t.Fatalf("only the batch published before the replacement may exist, got %+v", batches)
	}
}

func TestImportNeverEndsAnImportReplacedAfterItsLastBatch(t *testing.T) {
	s := newImporterSetup(t, textChunk(waHistorySync.HistorySync_RECENT, 100, 1), testLimits())
	s.store.begin("channel-1", activeImport)
	s.store.replaceAfterCount = &registry.HistoryImport{TenantID: "tenant-1", ImportID: "import-2"}

	if err := s.importChunk(t); err != nil {
		t.Fatalf("a replaced import is not an error, got %v", err)
	}

	want := []string{"download", "batch 1/1", "count", "release"}
	if got := s.log.all(); !reflect.DeepEqual(got, want) {
		t.Fatalf("steps %v, want %v", got, want)
	}
	if _, dones := s.publisher.published(); len(dones) != 0 {
		t.Fatalf("no done may be published for a replaced import, got %+v", dones)
	}
	if active, found, _ := s.store.ActiveHistoryImport(context.Background(), "channel-1"); !found || active.ImportID != "import-2" {
		t.Fatalf("the new import must stay active, got %+v found %v", active, found)
	}
}

func TestAcceptImportsTheChunksOfOneChannelOneAtATime(t *testing.T) {
	s := newImporterSetup(t, textChunk(waHistorySync.HistorySync_RECENT, 40, 1), testLimits())
	s.source.downloadHold = 20 * time.Millisecond

	for range 3 {
		s.importer.Accept("channel-1", s.source, &waE2E.HistorySyncNotification{})
	}
	s.importer.Close(5 * time.Second)

	if peak := s.source.downloadPeak.Load(); peak != 1 {
		t.Fatalf("chunks of the same channel must never overlap, peak %d", peak)
	}
	if released := s.log.count("release"); released != 3 {
		t.Fatalf("every accepted chunk is processed before Close returns, released %d", released)
	}
}

func TestCloseCancelsAChunkThatIsStillWaitingForItsConfirm(t *testing.T) {
	s := newImporterSetup(t, textChunk(waHistorySync.HistorySync_RECENT, 40, 1), testLimits())
	s.store.begin("channel-1", activeImport)
	s.publisher.hang = true

	s.importer.Accept("channel-1", s.source, &waE2E.HistorySyncNotification{})
	deadline := time.Now().Add(2 * time.Second)
	for s.log.count("download") == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	started := time.Now()
	s.importer.Close(50 * time.Millisecond)

	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("Close must cancel a stuck chunk after its timeout, took %s", elapsed)
	}
	if s.log.count("release") != 0 {
		t.Fatalf("a cancelled chunk is never released, got %v", s.log.all())
	}
}
