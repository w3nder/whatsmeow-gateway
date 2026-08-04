package call

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

// SampleRate is the calling stack's audio rate: 16 kHz mono.
const SampleRate = 16000

// wavHeaderSize is the canonical PCM WAV header, written up front with a zero
// data length and rewritten with the true count once the call ends.
const wavHeaderSize = 44

// Recorder captures a call's media to temp files and uploads them when the call
// ends.
//
// It writes to disk rather than to memory because a long call is large: half an
// hour of 16 kHz mono s16 is about 57 MB of audio alone, and a gateway holding
// one buffer per concurrent call would run itself out of memory.
//
// Both files are lazy. A call with no audio, or no video, leaves no object
// behind, so the backend can tell "nothing was captured" from "an empty file".
type Recorder struct {
	dir       string
	channelID string
	callID    string

	mu sync.Mutex

	audioFile *os.File
	audioLen  uint32

	videoFile *os.File
	videoLen  int

	// writeErr keeps the first write failure. Writes come from the calling
	// library's media goroutines, which have nowhere to report an error to, so
	// the failure is held here and surfaced by Finish.
	writeErr error

	finished bool
}

// NewRecorder prepares a recorder for one call. No file is created until the
// first frame arrives.
func NewRecorder(tmpDir, channelID, callID string) *Recorder {
	if tmpDir == "" {
		tmpDir = os.TempDir()
	}
	return &Recorder{dir: tmpDir, channelID: channelID, callID: callID}
}

// AudioKey is where this call's audio recording is stored.
func (r *Recorder) AudioKey() string {
	return fmt.Sprintf("calls/%s/%s.wav", r.channelID, r.callID)
}

// VideoKey is where this call's video recording is stored.
func (r *Recorder) VideoKey() string {
	return fmt.Sprintf("calls/%s/%s.h264", r.channelID, r.callID)
}

// WriteAudio appends one decoded mono frame as s16le PCM. It is safe to call
// from the media goroutine and never reports an error there: a failed write
// must not tear down a live call.
func (r *Recorder) WriteAudio(frame []float32) {
	if len(frame) == 0 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.finished || r.writeErr != nil {
		return
	}

	if r.audioFile == nil {
		f, err := os.CreateTemp(r.dir, "call-audio-*.wav")
		if err != nil {
			r.writeErr = fmt.Errorf("call: create audio recording: %w", err)
			return
		}
		// Placeholder header; Finish rewrites it with the real sizes.
		if _, err := f.Write(make([]byte, wavHeaderSize)); err != nil {
			_ = f.Close()
			_ = os.Remove(f.Name())
			r.writeErr = fmt.Errorf("call: write audio header: %w", err)
			return
		}
		r.audioFile = f
	}

	buf := make([]byte, len(frame)*2)
	for i, sample := range frame {
		binary.LittleEndian.PutUint16(buf[i*2:], uint16(pcmS16(sample)))
	}
	if _, err := r.audioFile.Write(buf); err != nil {
		r.writeErr = fmt.Errorf("call: write audio frame: %w", err)
		return
	}
	r.audioLen += uint32(len(buf))
}

// pcmS16 converts one float sample to signed 16-bit, clamping out-of-range
// input. Without the clamp an overshooting sample wraps into the opposite sign
// and turns into a loud click.
func pcmS16(sample float32) int16 {
	switch {
	case sample > 1:
		sample = 1
	case sample < -1:
		sample = -1
	}
	return int16(sample * 32767)
}

// WriteVideo appends one H.264 Annex-B access unit exactly as received. The
// gateway never transcodes video.
func (r *Recorder) WriteVideo(accessUnit []byte) {
	if len(accessUnit) == 0 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.finished || r.writeErr != nil {
		return
	}

	if r.videoFile == nil {
		f, err := os.CreateTemp(r.dir, "call-video-*.h264")
		if err != nil {
			r.writeErr = fmt.Errorf("call: create video recording: %w", err)
			return
		}
		r.videoFile = f
	}

	if _, err := r.videoFile.Write(accessUnit); err != nil {
		r.writeErr = fmt.Errorf("call: write video frame: %w", err)
		return
	}
	r.videoLen += len(accessUnit)
}

// Finish closes the temp files, uploads whatever was captured and reports the
// resulting media descriptors. Either may be nil when nothing was captured.
//
// The temp files are always removed, upload error or not: a gateway that keeps
// running has to not accumulate dead recordings on disk.
func (r *Recorder) Finish(ctx context.Context, store RecordingStore) (*Media, *Media, error) {
	r.mu.Lock()
	if r.finished {
		r.mu.Unlock()
		return nil, nil, nil
	}
	r.finished = true
	audioFile, audioLen := r.audioFile, r.audioLen
	videoFile := r.videoFile
	writeErr := r.writeErr
	r.mu.Unlock()

	var errs []error
	if writeErr != nil {
		errs = append(errs, writeErr)
	}

	audio, err := r.finishAudio(ctx, store, audioFile, audioLen)
	if err != nil {
		errs = append(errs, err)
	}
	video, err := r.finishVideo(ctx, store, videoFile)
	if err != nil {
		errs = append(errs, err)
	}

	return audio, video, errors.Join(errs...)
}

func (r *Recorder) finishAudio(ctx context.Context, store RecordingStore, f *os.File, dataLen uint32) (*Media, error) {
	if f == nil {
		return nil, nil
	}
	defer cleanup(f)

	if dataLen == 0 {
		return nil, nil
	}
	if _, err := f.WriteAt(wavHeader(dataLen), 0); err != nil {
		return nil, fmt.Errorf("call: finalize wav header: %w", err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("call: rewind audio recording: %w", err)
	}

	key := r.AudioKey()
	if err := store.PutStream(ctx, key, "audio/wav", f); err != nil {
		return nil, fmt.Errorf("call: upload audio recording: %w", err)
	}
	return &Media{
		Key:      key,
		MimeType: "audio/wav",
		Filename: r.callID + ".wav",
		Duration: int(dataLen) / (SampleRate * 2),
	}, nil
}

func (r *Recorder) finishVideo(ctx context.Context, store RecordingStore, f *os.File) (*Media, error) {
	if f == nil {
		return nil, nil
	}
	defer cleanup(f)

	if r.videoLen == 0 {
		return nil, nil
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("call: rewind video recording: %w", err)
	}

	key := r.VideoKey()
	if err := store.PutStream(ctx, key, "video/h264", f); err != nil {
		return nil, fmt.Errorf("call: upload video recording: %w", err)
	}
	return &Media{Key: key, MimeType: "video/h264", Filename: r.callID + ".h264"}, nil
}

func cleanup(f *os.File) {
	name := f.Name()
	_ = f.Close()
	_ = os.Remove(name)
}

// wavHeader builds the canonical 44-byte PCM WAV header for dataLen bytes of
// 16 kHz mono s16 samples.
func wavHeader(dataLen uint32) []byte {
	var h [wavHeaderSize]byte
	copy(h[0:4], "RIFF")
	binary.LittleEndian.PutUint32(h[4:8], 36+dataLen)
	copy(h[8:12], "WAVE")
	copy(h[12:16], "fmt ")
	binary.LittleEndian.PutUint32(h[16:20], 16) // PCM fmt chunk size
	binary.LittleEndian.PutUint16(h[20:22], 1)  // PCM
	binary.LittleEndian.PutUint16(h[22:24], 1)  // mono
	binary.LittleEndian.PutUint32(h[24:28], SampleRate)
	binary.LittleEndian.PutUint32(h[28:32], SampleRate*2) // byte rate
	binary.LittleEndian.PutUint16(h[32:34], 2)            // block align
	binary.LittleEndian.PutUint16(h[34:36], 16)           // bits per sample
	copy(h[36:40], "data")
	binary.LittleEndian.PutUint32(h[40:44], dataLen)
	return h[:]
}
