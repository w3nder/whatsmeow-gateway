package call_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/w3nder/whatsmeow-gateway/internal/call"
)

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

func (m *memStore) mime(key string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mimes[key]
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

const frameBytes = call.FrameSamples * 2

func wavSample(body []byte, i int) int16 {
	return int16(binary.LittleEndian.Uint16(body[44+i*2:]))
}

func wavData(t *testing.T, body []byte) int {
	t.Helper()
	declared := int(binary.LittleEndian.Uint32(body[40:44]))
	if got := len(body) - 44; got != declared {
		t.Errorf("wav declares %d data bytes but carries %d", declared, got)
	}
	if declared%frameBytes != 0 {
		t.Errorf("wav data = %d bytes, want a whole number of %d-byte frames", declared, frameBytes)
	}
	return declared
}

func operatorFrame(level int16) []byte {
	frame := make([]byte, frameBytes)
	for i := 0; i < len(frame); i += 2 {
		binary.LittleEndian.PutUint16(frame[i:], uint16(level))
	}
	return frame
}

func peerFrame(level float32) []float32 {
	frame := make([]float32, call.FrameSamples)
	for i := range frame {
		frame[i] = level
	}
	return frame
}

func TestRecorderWritesCanonicalWAV(t *testing.T) {
	dir := t.TempDir()
	rec := call.NewRecorder(dir, "chan1", "CALL1")

	rec.WritePeerAudio([]float32{1, 0, -1})

	store := newMemStore()
	got, err := rec.Finish(context.Background(), store)
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if got.Video != nil {
		t.Errorf("video = %+v, want nil when no video frame was written", got.Video)
	}
	if got.Audio == nil || got.Audio.Key != "calls/chan1/CALL1.wav" || got.Audio.MimeType != "audio/wav" {
		t.Fatalf("audio = %+v, want the wav key and mime", got.Audio)
	}

	body, ok := store.object("calls/chan1/CALL1.wav")
	if !ok {
		t.Fatal("the wav was never uploaded")
	}
	if wavData(t, body) < frameBytes {
		t.Fatalf("len(wav data) = %d, want at least one %d-byte frame", len(body)-44, frameBytes)
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
	if riffLen := binary.LittleEndian.Uint32(body[4:8]); riffLen != uint32(36+len(body)-44) {
		t.Errorf("riff size = %d, want %d", riffLen, 36+len(body)-44)
	}
	want := []int16{32767, 0, -32767}
	for i := range want {
		if got := wavSample(body, i); got != want[i] {
			t.Errorf("sample[%d] = %d, want %d", i, got, want[i])
		}
	}
	for i := 3; i < call.FrameSamples; i++ {
		if got := wavSample(body, i); got != 0 {
			t.Fatalf("sample[%d] = %d, want the frame padded with silence", i, got)
		}
	}
}

func TestRecorderClampsOutOfRangeSamples(t *testing.T) {
	dir := t.TempDir()
	rec := call.NewRecorder(dir, "chan1", "CALL1")
	rec.WritePeerAudio([]float32{2.5, -2.5})

	store := newMemStore()
	if _, err := rec.Finish(context.Background(), store); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	body, ok := store.object("calls/chan1/CALL1.wav")
	if !ok {
		t.Fatal("the wav was never uploaded")
	}
	if first, second := wavSample(body, 0), wavSample(body, 1); first != 32767 || second != -32767 {
		t.Errorf("samples = %d,%d, want 32767,-32767", first, second)
	}
}

func TestRecorderUploadsAllThreeTracks(t *testing.T) {
	dir := t.TempDir()
	rec := call.NewRecorder(dir, "chan1", "CALL1")

	rec.WritePeerAudio(peerFrame(0.5))
	rec.WriteOperatorAudio(operatorFrame(0x2000))

	store := newMemStore()
	got, err := rec.Finish(context.Background(), store)
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}

	for _, track := range []struct {
		name     string
		media    *call.Media
		key      string
		filename string
	}{
		{"mix", got.Audio, "calls/chan1/CALL1.wav", "CALL1.wav"},
		{"peer", got.PeerAudio, "calls/chan1/CALL1-peer.wav", "CALL1-peer.wav"},
		{"operator", got.OperatorAudio, "calls/chan1/CALL1-operator.wav", "CALL1-operator.wav"},
	} {
		if track.media == nil {
			t.Fatalf("%s media = nil, want a descriptor", track.name)
		}
		if track.media.Key != track.key {
			t.Errorf("%s key = %q, want %q", track.name, track.media.Key, track.key)
		}
		if track.media.MimeType != "audio/wav" {
			t.Errorf("%s mime = %q, want audio/wav", track.name, track.media.MimeType)
		}
		if track.media.Filename != track.filename {
			t.Errorf("%s filename = %q, want %q", track.name, track.media.Filename, track.filename)
		}
		if mime := store.mime(track.key); mime != "audio/wav" {
			t.Errorf("%s uploaded with mime %q, want audio/wav", track.name, mime)
		}
		if _, ok := store.object(track.key); !ok {
			t.Errorf("%s was never uploaded to %s", track.name, track.key)
		}
	}

	mix, _ := store.object(got.Audio.Key)
	peer, _ := store.object(got.PeerAudio.Key)
	operator, _ := store.object(got.OperatorAudio.Key)
	if len(mix) != len(peer) || len(peer) != len(operator) {
		t.Fatalf("track lengths = %d/%d/%d, want all equal: they share one timeline",
			len(mix), len(peer), len(operator))
	}

	if got := wavSample(peer, 0); got < 16382 || got > 16384 {
		t.Errorf("peer sample = %d, want the customer's 0.5", got)
	}
	if got := wavSample(operator, 0); got < 8190 || got > 8194 {
		t.Errorf("operator sample = %d, want the operator's 0.25", got)
	}
	if got := wavSample(mix, 0); got < 24574 || got > 24578 {
		t.Errorf("mix sample = %d, want the sum of both sides", got)
	}
}

func TestRecorderUploadsASilentTrackForTheSideThatNeverSpoke(t *testing.T) {
	dir := t.TempDir()
	rec := call.NewRecorder(dir, "chan1", "CALL1")

	rec.WritePeerAudio(peerFrame(0.5))

	store := newMemStore()
	got, err := rec.Finish(context.Background(), store)
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if got.OperatorAudio == nil {
		t.Fatal("operator media = nil, want a silent track rather than nothing")
	}

	operator, ok := store.object("calls/chan1/CALL1-operator.wav")
	if !ok {
		t.Fatal("the operator track was never uploaded")
	}
	peer, _ := store.object("calls/chan1/CALL1-peer.wav")
	if len(operator) != len(peer) {
		t.Fatalf("operator track is %d bytes and peer is %d: the silent side must span the same time",
			len(operator), len(peer))
	}
	if data := operator[44:]; !bytes.Equal(data, make([]byte, len(data))) {
		t.Error("the operator track is not silence, but the operator never spoke")
	}
}

func TestRecorderWritesVideoVerbatim(t *testing.T) {
	dir := t.TempDir()
	rec := call.NewRecorder(dir, "chan1", "CALL1")
	rec.WriteVideo([]byte{0, 0, 0, 1, 0x67, 0x42})
	rec.WriteVideo([]byte{0, 0, 0, 1, 0x65, 0x88})

	store := newMemStore()
	got, err := rec.Finish(context.Background(), store)
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if got.Audio != nil || got.PeerAudio != nil || got.OperatorAudio != nil {
		t.Errorf("audio = %+v/%+v/%+v, want all nil when no audio frame was written",
			got.Audio, got.PeerAudio, got.OperatorAudio)
	}
	if got.Video == nil || got.Video.Key != "calls/chan1/CALL1.h264" || got.Video.MimeType != "video/h264" {
		t.Fatalf("video = %+v, want the h264 key and mime", got.Video)
	}
	want := []byte{0, 0, 0, 1, 0x67, 0x42, 0, 0, 0, 1, 0x65, 0x88}
	body, ok := store.object("calls/chan1/CALL1.h264")
	if !ok {
		t.Fatal("the h264 was never uploaded")
	}
	if !bytes.Equal(body, want) {
		t.Errorf("h264 = % x, want % x", body, want)
	}
}

func TestRecorderUploadsNothingWhenNoFrames(t *testing.T) {
	dir := t.TempDir()
	rec := call.NewRecorder(dir, "chan1", "CALL1")

	time.Sleep(200 * time.Millisecond)

	store := newMemStore()
	got, err := rec.Finish(context.Background(), store)
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if got != (call.Recording{}) {
		t.Errorf("recording = %+v, want every descriptor nil", got)
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

func TestRecorderRemovesTempFilesOnUploadError(t *testing.T) {
	dir := t.TempDir()
	rec := call.NewRecorder(dir, "chan1", "CALL1")
	rec.WritePeerAudio([]float32{0.5})
	rec.WriteOperatorAudio(operatorFrame(0x2000))
	rec.WriteVideo([]byte{0, 0, 0, 1, 0x65})

	store := newMemStore()
	store.fail(errors.New("bucket is on fire"))
	if _, err := rec.Finish(context.Background(), store); err == nil {
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

func TestRecorderIsSafeForConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	rec := call.NewRecorder(dir, "chan1", "CALL1")

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			rec.WritePeerAudio([]float32{0.1, 0.2})
		}()
		go func() {
			defer wg.Done()
			rec.WriteOperatorAudio([]byte{0x00, 0x10, 0x00, 0x10})
		}()
		go func() {
			defer wg.Done()
			rec.WriteVideo([]byte{0, 0, 0, 1, 0x41})
		}()
	}
	wg.Wait()

	store := newMemStore()
	got, err := rec.Finish(context.Background(), store)
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}

	mix, _ := store.object(got.Audio.Key)
	peer, _ := store.object(got.PeerAudio.Key)
	operator, _ := store.object(got.OperatorAudio.Key)
	if len(mix) != len(peer) || len(peer) != len(operator) {
		t.Errorf("track lengths = %d/%d/%d, want all equal", len(mix), len(peer), len(operator))
	}
	wavData(t, mix)

	videoBody, _ := store.object(got.Video.Key)
	if len(videoBody) != 50*5 {
		t.Errorf("h264 size = %d, want %d", len(videoBody), 50*5)
	}
}

func TestRecorderClockDoesNotOutliveTheCall(t *testing.T) {
	dir := t.TempDir()
	store := newMemStore()

	warmup := call.NewRecorder(dir, "chan1", "WARMUP")
	warmup.WritePeerAudio([]float32{0.5})
	if _, err := warmup.Finish(context.Background(), store); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	baseline := runtime.NumGoroutine()

	for i := 0; i < 20; i++ {
		rec := call.NewRecorder(dir, "chan1", "CALL1")
		rec.WritePeerAudio([]float32{0.5})
		rec.WriteOperatorAudio(operatorFrame(0x2000))
		if _, err := rec.Finish(context.Background(), store); err != nil {
			t.Fatalf("Finish: %v", err)
		}
	}

	if got := runtime.NumGoroutine(); got > baseline {
		t.Errorf("goroutines = %d after 20 finished calls, want no more than the %d baseline: "+
			"a recorder's clock is outliving its call", got, baseline)
	}
}
