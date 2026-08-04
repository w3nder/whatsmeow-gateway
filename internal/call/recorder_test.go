package call_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
	"testing"

	"github.com/w3nder/whatsmeow-gateway/internal/call"
)

// memStore is a RecordingStore that keeps uploads in memory.
type memStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	mimes   map[string]string
	err     error
	gate    chan struct{}
}

func newMemStore() *memStore {
	return &memStore{objects: map[string][]byte{}, mimes: map[string]string{}}
}

// blockPuts makes every PutStream call wait until release is called. It
// stands in for a slow object store, so a test can observe what happens while
// an upload is still in flight.
func (m *memStore) blockPuts() (release func()) {
	gate := make(chan struct{})
	var once sync.Once
	m.mu.Lock()
	m.gate = gate
	m.mu.Unlock()
	return func() {
		once.Do(func() { close(gate) })
	}
}

func (m *memStore) PutStream(_ context.Context, key, mime string, r io.Reader) error {
	m.mu.Lock()
	failure := m.err
	gate := m.gate
	m.mu.Unlock()
	if gate != nil {
		<-gate
	}
	if failure != nil {
		return failure
	}
	body, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[key] = body
	m.mimes[key] = mime
	return nil
}

func (m *memStore) object(key string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	body, ok := m.objects[key]
	return body, ok
}

func (m *memStore) objectCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.objects)
}

func (m *memStore) fail(err error) {
	m.mu.Lock()
	m.err = err
	m.mu.Unlock()
}

func TestRecorderWritesCanonicalWAV(t *testing.T) {
	dir := t.TempDir()
	rec := call.NewRecorder(dir, "chan1", "CALL1")

	// Full-scale positive, silence, full-scale negative.
	rec.WriteAudio([]float32{1, 0, -1})

	store := newMemStore()
	audio, video, err := rec.Finish(context.Background(), store)
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if video != nil {
		t.Errorf("video = %+v, want nil when no video frame was written", video)
	}
	if audio == nil || audio.Key != "calls/chan1/CALL1.wav" || audio.MimeType != "audio/wav" {
		t.Fatalf("audio = %+v, want the wav key and mime", audio)
	}

	body, ok := store.object("calls/chan1/CALL1.wav")
	if !ok {
		t.Fatal("the wav was never uploaded")
	}
	if len(body) != 44+6 {
		t.Fatalf("len(wav) = %d, want 50 (44-byte header + 3 samples)", len(body))
	}
	if string(body[0:4]) != "RIFF" || string(body[8:12]) != "WAVE" {
		t.Errorf("header = %q, want a RIFF/WAVE header", body[0:12])
	}
	if rate := binary.LittleEndian.Uint32(body[24:28]); rate != 16000 {
		t.Errorf("sample rate = %d, want 16000", rate)
	}
	if ch := binary.LittleEndian.Uint16(body[22:24]); ch != 1 {
		t.Errorf("channels = %d, want 1 (mono)", ch)
	}
	if bits := binary.LittleEndian.Uint16(body[34:36]); bits != 16 {
		t.Errorf("bits per sample = %d, want 16", bits)
	}
	if dataLen := binary.LittleEndian.Uint32(body[40:44]); dataLen != 6 {
		t.Errorf("data chunk size = %d, want 6", dataLen)
	}
	if riffLen := binary.LittleEndian.Uint32(body[4:8]); riffLen != 36+6 {
		t.Errorf("riff size = %d, want 42", riffLen)
	}
	samples := []int16{
		int16(binary.LittleEndian.Uint16(body[44:46])),
		int16(binary.LittleEndian.Uint16(body[46:48])),
		int16(binary.LittleEndian.Uint16(body[48:50])),
	}
	want := []int16{32767, 0, -32767}
	for i := range want {
		if samples[i] != want[i] {
			t.Errorf("sample[%d] = %d, want %d", i, samples[i], want[i])
		}
	}
}

// float32 outside [-1,1] must clamp, not wrap around into the opposite sign.
func TestRecorderClampsOutOfRangeSamples(t *testing.T) {
	dir := t.TempDir()
	rec := call.NewRecorder(dir, "chan1", "CALL1")
	rec.WriteAudio([]float32{2.5, -2.5})

	store := newMemStore()
	if _, _, err := rec.Finish(context.Background(), store); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	body, ok := store.object("calls/chan1/CALL1.wav")
	if !ok {
		t.Fatal("the wav was never uploaded")
	}
	first := int16(binary.LittleEndian.Uint16(body[44:46]))
	second := int16(binary.LittleEndian.Uint16(body[46:48]))
	if first != 32767 || second != -32767 {
		t.Errorf("samples = %d,%d, want 32767,-32767", first, second)
	}
}

func TestRecorderWritesVideoVerbatim(t *testing.T) {
	dir := t.TempDir()
	rec := call.NewRecorder(dir, "chan1", "CALL1")
	rec.WriteVideo([]byte{0, 0, 0, 1, 0x67, 0x42})
	rec.WriteVideo([]byte{0, 0, 0, 1, 0x65, 0x88})

	store := newMemStore()
	audio, video, err := rec.Finish(context.Background(), store)
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if audio != nil {
		t.Errorf("audio = %+v, want nil when no audio frame was written", audio)
	}
	if video == nil || video.Key != "calls/chan1/CALL1.h264" || video.MimeType != "video/h264" {
		t.Fatalf("video = %+v, want the h264 key and mime", video)
	}
	want := []byte{0, 0, 0, 1, 0x67, 0x42, 0, 0, 0, 1, 0x65, 0x88}
	got, ok := store.object("calls/chan1/CALL1.h264")
	if !ok {
		t.Fatal("the h264 was never uploaded")
	}
	if !bytes.Equal(got, want) {
		t.Errorf("h264 = % x, want % x", got, want)
	}
}

// A call that captured nothing must not leave an empty object in the bucket.
func TestRecorderUploadsNothingWhenNoFrames(t *testing.T) {
	dir := t.TempDir()
	rec := call.NewRecorder(dir, "chan1", "CALL1")

	store := newMemStore()
	audio, video, err := rec.Finish(context.Background(), store)
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if audio != nil || video != nil {
		t.Errorf("audio=%+v video=%+v, want both nil", audio, video)
	}
	if store.objectCount() != 0 {
		t.Errorf("uploaded %d objects, want none", store.objectCount())
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("a call with no frames left %d temp files behind", len(entries))
	}
}

// The temp file must go away even when the upload fails, or a long-running
// gateway fills its disk with dead recordings.
func TestRecorderRemovesTempFilesOnUploadError(t *testing.T) {
	dir := t.TempDir()
	rec := call.NewRecorder(dir, "chan1", "CALL1")
	rec.WriteAudio([]float32{0.5})

	store := newMemStore()
	store.fail(errors.New("bucket is on fire"))
	if _, _, err := rec.Finish(context.Background(), store); err == nil {
		t.Fatal("Finish err = nil, want the upload error")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("leftover temp files: %v", names)
	}
}

// Recording runs on the library's media goroutines, so the writes must be safe
// to call concurrently.
func TestRecorderIsSafeForConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	rec := call.NewRecorder(dir, "chan1", "CALL1")

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			rec.WriteAudio([]float32{0.1, 0.2})
		}()
		go func() {
			defer wg.Done()
			rec.WriteVideo([]byte{0, 0, 0, 1, 0x41})
		}()
	}
	wg.Wait()

	store := newMemStore()
	audio, video, err := rec.Finish(context.Background(), store)
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	body, _ := store.object(audio.Key)
	if got := binary.LittleEndian.Uint32(body[40:44]); got != 50*2*2 {
		t.Errorf("wav data size = %d, want %d", got, 50*2*2)
	}
	videoBody, _ := store.object(video.Key)
	if len(videoBody) != 50*5 {
		t.Errorf("h264 size = %d, want %d", len(videoBody), 50*5)
	}
}
