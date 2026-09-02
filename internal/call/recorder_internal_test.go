package call

import (
	"context"
	"encoding/binary"
	"io"
	"sync"
	"testing"
	"time"
)

type trackStore struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newTrackStore() *trackStore {
	return &trackStore{objects: map[string][]byte{}}
}

func (s *trackStore) PutStream(_ context.Context, key, _ string, r io.Reader) error {
	body, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[key] = body
	return nil
}

func (s *trackStore) get(t *testing.T, key string) []byte {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	body, ok := s.objects[key]
	if !ok {
		t.Fatalf("%s was never uploaded", key)
	}
	return body
}

func gridRecorder(t *testing.T) *Recorder {
	t.Helper()
	return newRecorder(t.TempDir(), "chan1", "CALL1", time.Hour)
}

func frameLevel(body []byte, frame int) int16 {
	return int16(binary.LittleEndian.Uint16(body[wavHeaderSize+frame*FrameSamples*2:]))
}

func frameCount(body []byte) int {
	return (len(body) - wavHeaderSize) / (FrameSamples * 2)
}

func frameIsSilent(body []byte, frame int) bool {
	start := wavHeaderSize + frame*FrameSamples*2
	for i := 0; i < FrameSamples*2; i++ {
		if body[start+i] != 0 {
			return false
		}
	}
	return true
}

func s16Frame(level int16) []byte {
	frame := make([]byte, FrameSamples*2)
	for i := 0; i < len(frame); i += 2 {
		binary.LittleEndian.PutUint16(frame[i:], uint16(level))
	}
	return frame
}

func floatFrame(level float32) []float32 {
	frame := make([]float32, FrameSamples)
	for i := range frame {
		frame[i] = level
	}
	return frame
}

func near(got, want int16) bool {
	diff := int(got) - int(want)
	return diff >= -2 && diff <= 2
}

func TestRecorderMixSaturatesRatherThanWrapping(t *testing.T) {
	for _, tc := range []struct {
		name     string
		peer     float32
		operator int16
		want     int16
	}{
		{"both at positive full scale", 1, 32767, 32767},
		{"both at negative full scale", -1, -32767, -32767},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := gridRecorder(t)
			rec.WritePeerAudio(floatFrame(tc.peer))
			rec.WriteOperatorAudio(s16Frame(tc.operator))
			rec.writeFrame()

			store := newTrackStore()
			got, err := rec.Finish(context.Background(), store)
			if err != nil {
				t.Fatalf("Finish: %v", err)
			}

			mix := store.get(t, got.Audio.Key)
			if level := frameLevel(mix, 0); level != tc.want {
				t.Errorf("mix = %d, want %d: the sum must clamp, not wrap", level, tc.want)
			}
			if level := frameLevel(store.get(t, got.PeerAudio.Key), 0); !near(level, tc.want) {
				t.Errorf("peer track = %d, want %d", level, tc.want)
			}
			if level := frameLevel(store.get(t, got.OperatorAudio.Key), 0); !near(level, tc.want) {
				t.Errorf("operator track = %d, want %d", level, tc.want)
			}
		})
	}
}

func TestRecorderKeepsTheTracksAlignedAcrossSilence(t *testing.T) {
	rec := gridRecorder(t)

	for _, tick := range []struct {
		peer     bool
		operator bool
	}{
		{peer: true},
		{peer: true},
		{},
		{},
		{},
		{operator: true},
		{peer: true, operator: true},
	} {
		if tick.peer {
			rec.WritePeerAudio(floatFrame(0.5))
		}
		if tick.operator {
			rec.WriteOperatorAudio(s16Frame(0x2000))
		}
		rec.writeFrame()
	}

	store := newTrackStore()
	got, err := rec.Finish(context.Background(), store)
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}

	mix := store.get(t, got.Audio.Key)
	peer := store.get(t, got.PeerAudio.Key)
	operator := store.get(t, got.OperatorAudio.Key)

	if len(mix) != len(peer) || len(peer) != len(operator) {
		t.Fatalf("track lengths = %d/%d/%d, want all equal", len(mix), len(peer), len(operator))
	}
	if got := frameCount(mix); got != 7 {
		t.Fatalf("frames = %d, want the 7 ticks that were written", got)
	}

	const (
		peerLevel     = 16383
		operatorLevel = 8192
		bothLevel     = 24575
	)
	for frame, want := range []struct {
		peer, operator, mix int16
	}{
		0: {peer: peerLevel, mix: peerLevel},
		1: {peer: peerLevel, mix: peerLevel},
		2: {},
		3: {},
		4: {},
		5: {operator: operatorLevel, mix: operatorLevel},
		6: {peer: peerLevel, operator: operatorLevel, mix: bothLevel},
	} {
		if want.peer == 0 && !frameIsSilent(peer, frame) {
			t.Errorf("peer frame %d is not silence, but the customer said nothing on that tick", frame)
		}
		if want.operator == 0 && !frameIsSilent(operator, frame) {
			t.Errorf("operator frame %d is not silence, but the operator said nothing on that tick", frame)
		}
		if want.mix == 0 && !frameIsSilent(mix, frame) {
			t.Errorf("mix frame %d is not silence, but neither side said anything on that tick", frame)
		}
		if want.peer != 0 && !near(frameLevel(peer, frame), want.peer) {
			t.Errorf("peer frame %d = %d, want %d", frame, frameLevel(peer, frame), want.peer)
		}
		if want.operator != 0 && !near(frameLevel(operator, frame), want.operator) {
			t.Errorf("operator frame %d = %d, want %d", frame, frameLevel(operator, frame), want.operator)
		}
		if want.mix != 0 && !near(frameLevel(mix, frame), want.mix) {
			t.Errorf("mix frame %d = %d, want %d", frame, frameLevel(mix, frame), want.mix)
		}
	}
}

func TestRecorderFlushesWhatIsStillBufferedWhenTheCallEnds(t *testing.T) {
	rec := gridRecorder(t)
	rec.WritePeerAudio(floatFrame(0.5))

	store := newTrackStore()
	got, err := rec.Finish(context.Background(), store)
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}

	peer := store.get(t, got.PeerAudio.Key)
	if frames := frameCount(peer); frames != 1 {
		t.Fatalf("frames = %d, want the one frame that was still buffered", frames)
	}
	if level := frameLevel(peer, 0); !near(level, 16383) {
		t.Errorf("flushed frame = %d, want the buffered 0.5", level)
	}
}

func TestRecorderFinishJoinsTheClock(t *testing.T) {
	rec := gridRecorder(t)
	rec.WritePeerAudio(floatFrame(0.5))

	rec.mu.Lock()
	done := rec.done
	rec.mu.Unlock()
	if done == nil {
		t.Fatal("the first frame did not start the clock")
	}

	if _, err := rec.Finish(context.Background(), newTrackStore()); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	select {
	case <-done:
	default:
		t.Error("the clock goroutine was still running when Finish returned")
	}
}

func TestRecorderDoesNotStartTheClockUntilAudioArrives(t *testing.T) {
	rec := gridRecorder(t)

	rec.mu.Lock()
	started := rec.started
	rec.mu.Unlock()
	if started {
		t.Error("the clock started on a call that had not sent a frame")
	}

	if _, err := rec.Finish(context.Background(), newTrackStore()); err != nil {
		t.Fatalf("Finish: %v", err)
	}
}

func TestAudioRingIsBounded(t *testing.T) {
	var ring audioRing
	for i := 0; i < 200; i++ {
		ring.write(floatFrame(0.5))
	}
	if len(ring.samples) != recorderRingSamples {
		t.Errorf("ring holds %d samples, want it capped at %d", len(ring.samples), recorderRingSamples)
	}
}

func TestAudioRingTakePadsWithSilence(t *testing.T) {
	var ring audioRing
	ring.write([]float32{1, 1, 1})

	dst := make([]float32, FrameSamples)
	for i := range dst {
		dst[i] = 0.9
	}
	ring.take(dst)

	for i := 0; i < 3; i++ {
		if dst[i] != 1 {
			t.Fatalf("dst[%d] = %v, want the queued sample", i, dst[i])
		}
	}
	for i := 3; i < len(dst); i++ {
		if dst[i] != 0 {
			t.Fatalf("dst[%d] = %v, want silence", i, dst[i])
		}
	}
	if !ring.empty() {
		t.Error("the ring still holds samples it already handed over")
	}
}
