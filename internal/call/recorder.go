package call

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/purpshell/meowcaller"
)

const (
	SampleRate   = meowcaller.SampleRate
	FrameSamples = meowcaller.FrameSamples
)

const frameInterval = FrameSamples * time.Second / SampleRate

const recorderRingSamples = 50 * FrameSamples

const wavHeaderSize = 44

type Recording struct {
	Audio         *Media
	PeerAudio     *Media
	OperatorAudio *Media
	Video         *Media
}

type Recorder struct {
	dir       string
	channelID string
	callID    string
	interval  time.Duration

	mu       sync.Mutex
	peer     audioRing
	operator audioRing
	started  bool
	closing  bool
	stop     chan struct{}
	done     chan struct{}

	mixFile       *os.File
	peerFile      *os.File
	operatorFile  *os.File
	frames        uint32
	peerFrame     []float32
	operatorFrame []float32
	mixFrame      []float32
	pcm           []byte
	audioErr      error

	videoMu     sync.Mutex
	videoFile   *os.File
	videoLen    int
	videoErr    error
	videoClosed bool
}

func NewRecorder(tmpDir, channelID, callID string) *Recorder {
	return newRecorder(tmpDir, channelID, callID, frameInterval)
}

func newRecorder(tmpDir, channelID, callID string, interval time.Duration) *Recorder {
	if tmpDir == "" {
		tmpDir = os.TempDir()
	}
	return &Recorder{
		dir:           tmpDir,
		channelID:     channelID,
		callID:        callID,
		interval:      interval,
		peerFrame:     make([]float32, FrameSamples),
		operatorFrame: make([]float32, FrameSamples),
		mixFrame:      make([]float32, FrameSamples),
		pcm:           make([]byte, FrameSamples*2),
	}
}

func (r *Recorder) AudioKey() string {
	return fmt.Sprintf("calls/%s/%s.wav", r.channelID, r.callID)
}

func (r *Recorder) PeerAudioKey() string {
	return fmt.Sprintf("calls/%s/%s-peer.wav", r.channelID, r.callID)
}

func (r *Recorder) OperatorAudioKey() string {
	return fmt.Sprintf("calls/%s/%s-operator.wav", r.channelID, r.callID)
}

func (r *Recorder) VideoKey() string {
	return fmt.Sprintf("calls/%s/%s.h264", r.channelID, r.callID)
}

func (r *Recorder) WritePeerAudio(frame []float32) {
	if len(frame) == 0 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closing {
		return
	}
	r.peer.write(frame)
	r.startLocked()
}

func (r *Recorder) WriteOperatorAudio(frame []byte) {
	if len(frame) < 2 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closing {
		return
	}
	r.operator.write16(frame)
	r.startLocked()
}

func (r *Recorder) startLocked() {
	if r.started {
		return
	}
	r.started = true
	r.stop = make(chan struct{})
	r.done = make(chan struct{})
	go r.run()
}

func (r *Recorder) run() {
	defer close(r.done)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stop:
			r.drain()
			return
		case <-ticker.C:
			r.writeFrame()
		}
	}
}

func (r *Recorder) drain() {
	for r.audioErr == nil {
		r.mu.Lock()
		empty := r.peer.empty() && r.operator.empty()
		r.mu.Unlock()
		if empty {
			return
		}
		r.writeFrame()
	}
}

func (r *Recorder) writeFrame() {
	if r.audioErr != nil {
		return
	}

	r.mu.Lock()
	r.peer.take(r.peerFrame)
	r.operator.take(r.operatorFrame)
	r.mu.Unlock()

	for i := range r.mixFrame {
		r.mixFrame[i] = r.peerFrame[i] + r.operatorFrame[i]
	}

	if r.mixFile == nil {
		if err := r.openTracks(); err != nil {
			r.audioErr = err
			return
		}
	}

	for _, track := range []struct {
		file  *os.File
		frame []float32
	}{
		{r.mixFile, r.mixFrame},
		{r.peerFile, r.peerFrame},
		{r.operatorFile, r.operatorFrame},
	} {
		if err := r.writeTrack(track.file, track.frame); err != nil {
			r.audioErr = err
			return
		}
	}
	r.frames++
}

func (r *Recorder) writeTrack(f *os.File, frame []float32) error {
	for i, sample := range frame {
		binary.LittleEndian.PutUint16(r.pcm[i*2:], uint16(pcmS16(sample)))
	}
	if _, err := f.Write(r.pcm); err != nil {
		return fmt.Errorf("call: write audio frame: %w", err)
	}
	return nil
}

func (r *Recorder) openTracks() error {
	mix, err := openTrack(r.dir, "call-audio-*.wav")
	if err != nil {
		return err
	}
	peer, err := openTrack(r.dir, "call-peer-*.wav")
	if err != nil {
		cleanup(mix)
		return err
	}
	operator, err := openTrack(r.dir, "call-operator-*.wav")
	if err != nil {
		cleanup(mix)
		cleanup(peer)
		return err
	}

	r.mixFile, r.peerFile, r.operatorFile = mix, peer, operator
	return nil
}

func openTrack(dir, pattern string) (*os.File, error) {
	f, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return nil, fmt.Errorf("call: create audio recording: %w", err)
	}
	if _, err := f.Write(make([]byte, wavHeaderSize)); err != nil {
		cleanup(f)
		return nil, fmt.Errorf("call: write audio header: %w", err)
	}
	return f, nil
}

func pcmS16(sample float32) int16 {
	switch {
	case sample > 1:
		sample = 1
	case sample < -1:
		sample = -1
	}
	return int16(sample * 32767)
}

func (r *Recorder) WriteVideo(accessUnit []byte) {
	if len(accessUnit) == 0 {
		return
	}

	r.videoMu.Lock()
	defer r.videoMu.Unlock()
	if r.videoClosed || r.videoErr != nil {
		return
	}

	if r.videoFile == nil {
		f, err := os.CreateTemp(r.dir, "call-video-*.h264")
		if err != nil {
			r.videoErr = fmt.Errorf("call: create video recording: %w", err)
			return
		}
		r.videoFile = f
	}

	if _, err := r.videoFile.Write(accessUnit); err != nil {
		r.videoErr = fmt.Errorf("call: write video frame: %w", err)
		return
	}
	r.videoLen += len(accessUnit)
}

func (r *Recorder) Finish(ctx context.Context, store RecordingStore) (Recording, error) {
	r.mu.Lock()
	if r.closing {
		r.mu.Unlock()
		return Recording{}, nil
	}
	r.closing = true
	stop, done := r.stop, r.done
	r.mu.Unlock()

	if stop != nil {
		close(stop)
		<-done
	}

	r.videoMu.Lock()
	r.videoClosed = true
	videoFile, videoLen, videoErr := r.videoFile, r.videoLen, r.videoErr
	r.videoMu.Unlock()

	var errs []error
	for _, err := range []error{r.audioErr, videoErr} {
		if err != nil {
			errs = append(errs, err)
		}
	}

	dataLen := r.frames * FrameSamples * 2

	var rec Recording
	for _, track := range []struct {
		file     *os.File
		key      string
		filename string
		into     **Media
	}{
		{r.mixFile, r.AudioKey(), r.callID + ".wav", &rec.Audio},
		{r.peerFile, r.PeerAudioKey(), r.callID + "-peer.wav", &rec.PeerAudio},
		{r.operatorFile, r.OperatorAudioKey(), r.callID + "-operator.wav", &rec.OperatorAudio},
	} {
		media, err := r.finishTrack(ctx, store, track.file, track.key, track.filename, dataLen)
		if err != nil {
			errs = append(errs, err)
		}
		*track.into = media
	}

	video, err := r.finishVideo(ctx, store, videoFile, videoLen)
	if err != nil {
		errs = append(errs, err)
	}
	rec.Video = video

	return rec, errors.Join(errs...)
}

func (r *Recorder) finishTrack(
	ctx context.Context,
	store RecordingStore,
	f *os.File,
	key, filename string,
	dataLen uint32,
) (*Media, error) {
	if f == nil {
		return nil, nil
	}
	defer cleanup(f)

	if dataLen == 0 {
		return nil, nil
	}
	if err := f.Truncate(int64(wavHeaderSize + dataLen)); err != nil {
		return nil, fmt.Errorf("call: truncate audio recording: %w", err)
	}
	if _, err := f.WriteAt(wavHeader(dataLen), 0); err != nil {
		return nil, fmt.Errorf("call: finalize wav header: %w", err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("call: rewind audio recording: %w", err)
	}

	if err := store.PutStream(ctx, key, "audio/wav", f); err != nil {
		return nil, fmt.Errorf("call: upload audio recording: %w", err)
	}
	return &Media{
		Key:      key,
		MimeType: "audio/wav",
		Filename: filename,
		Duration: int(dataLen) / (SampleRate * 2),
	}, nil
}

func (r *Recorder) finishVideo(ctx context.Context, store RecordingStore, f *os.File, dataLen int) (*Media, error) {
	if f == nil {
		return nil, nil
	}
	defer cleanup(f)

	if dataLen == 0 {
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

type audioRing struct {
	samples []float32
}

func (r *audioRing) write(frame []float32) {
	r.samples = append(r.samples, frame...)
	r.trim()
}

func (r *audioRing) write16(frame []byte) {
	for i := 0; i+1 < len(frame); i += 2 {
		sample := int16(binary.LittleEndian.Uint16(frame[i:]))
		r.samples = append(r.samples, float32(sample)/32767)
	}
	r.trim()
}

func (r *audioRing) trim() {
	if over := len(r.samples) - recorderRingSamples; over > 0 {
		r.samples = append(r.samples[:0], r.samples[over:]...)
	}
}

func (r *audioRing) take(dst []float32) {
	n := copy(dst, r.samples)
	r.samples = append(r.samples[:0], r.samples[n:]...)
	clear(dst[n:])
}

func (r *audioRing) empty() bool { return len(r.samples) == 0 }

func wavHeader(dataLen uint32) []byte {
	var h [wavHeaderSize]byte
	copy(h[0:4], "RIFF")
	binary.LittleEndian.PutUint32(h[4:8], 36+dataLen)
	copy(h[8:12], "WAVE")
	copy(h[12:16], "fmt ")
	binary.LittleEndian.PutUint32(h[16:20], 16)
	binary.LittleEndian.PutUint16(h[20:22], 1)
	binary.LittleEndian.PutUint16(h[22:24], 1)
	binary.LittleEndian.PutUint32(h[24:28], SampleRate)
	binary.LittleEndian.PutUint32(h[28:32], SampleRate*2)
	binary.LittleEndian.PutUint16(h[32:34], 2)
	binary.LittleEndian.PutUint16(h[34:36], 16)
	copy(h[36:40], "data")
	binary.LittleEndian.PutUint32(h[40:44], dataLen)
	return h[:]
}
