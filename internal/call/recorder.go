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
	// SampleRate is the calling stack's audio rate: 16 kHz mono.
	//
	// It and FrameSamples are taken from the library rather than restated here.
	// Every sample the recorder mixes came off that stack, so a number that
	// drifted from it would not fail -- it would silently stretch or squash the
	// recording, and the mix would slide the two voices apart over the length
	// of a call.
	SampleRate = meowcaller.SampleRate
	// FrameSamples is one media frame: 960 samples, 60 ms at SampleRate.
	FrameSamples = meowcaller.FrameSamples
)

// frameInterval is the recorder's clock. One tick writes one frame of each
// side, so it has to be the frame's own duration and nothing else.
const frameInterval = FrameSamples * time.Second / SampleRate

// recorderRingSamples bounds how much of either side may wait for the clock:
// three seconds, the same order the rest of this package allows live audio.
//
// A ring that overflows means the clock goroutine has been starved for three
// seconds, at which point the recording has a hole in it whatever we do. It
// drops the oldest samples, matching the policy in outbound.go, so a starved
// recorder cannot take the process's memory down with it.
const recorderRingSamples = 50 * FrameSamples

// wavHeaderSize is the canonical PCM WAV header, written up front with a zero
// data length and rewritten with the true count once the call ends.
const wavHeaderSize = 44

// Recording is what a finished call left behind. Any field may be nil: a call
// captures audio, video, both or neither.
type Recording struct {
	// Audio is the two sides mixed to mono, and is the track a player in the
	// chat plays.
	Audio *Media
	// PeerAudio and OperatorAudio are the same span of time with one side on
	// each. They are the point of the split: fed to transcription individually
	// they say who spoke, with no diarisation step in between.
	PeerAudio     *Media
	OperatorAudio *Media
	// Video is the peer's H.264, stored verbatim.
	Video *Media
}

// Recorder captures a call's media to temp files and uploads them when the call
// ends.
//
// It produces three audio tracks rather than one, and the three share a single
// timeline. That is why it is clock-driven and not callback-driven: the two
// sides arrive on unrelated schedules -- the customer's frames when WhatsApp's
// relay delivers them, the operator's when a browser gets round to sending
// them -- so writing each one as it landed would let the tracks drift apart by
// however much the two schedules differ, and the mix would have the two voices
// sliding over each other. Instead each side lands in a ring buffer and a
// ticker writes exactly one frame of each per tick, silence when a ring is
// empty. A side that says nothing for ten seconds costs ten seconds of silence
// in its own track, in exactly the right place.
//
// It writes to disk rather than to memory because a long call is large: half an
// hour of 16 kHz mono s16 is about 57 MB per track, and a gateway holding three
// buffers per concurrent call would run itself out of memory.
//
// Everything is lazy, and the clock does not start until a frame arrives from
// one side or the other. A call with no audio at all, or no video, leaves no
// object behind, so the backend can tell "nothing was captured" from "an empty
// file". Once the clock has started all three audio tracks exist, including for
// a side that never spoke: a silent track of the right length says "this side
// said nothing", where a missing object cannot be told apart from an upload
// that failed or a gateway too old to produce it.
type Recorder struct {
	dir       string
	channelID string
	callID    string
	interval  time.Duration

	// mu guards the rings and the clock's lifecycle, and is only ever held for
	// a memcpy. Both producers are the calling library's media goroutines --
	// the relay's decoder on one side, the transmit loop on the other -- and
	// nothing in this package may make either of them wait. That is the whole
	// reason the disk writes moved onto the clock's goroutine, outside this
	// lock, instead of happening in the producer as they used to.
	mu       sync.Mutex
	peer     audioRing
	operator audioRing
	started  bool
	closing  bool
	stop     chan struct{}
	done     chan struct{}

	// Everything from here to videoMu belongs to the clock goroutine alone. It
	// is the only writer, and Finish reads it only after joining the goroutine,
	// so none of it is locked -- and none of it wants to be, since a file write
	// must never be reachable while mu is held.
	mixFile      *os.File
	peerFile     *os.File
	operatorFile *os.File
	// frames counts ticks written. All three tracks are written on every tick,
	// so one count sizes all three.
	frames uint32
	// The frame scratch is refilled each tick rather than reallocated: a call
	// ticks about a thousand times a minute.
	peerFrame     []float32
	operatorFrame []float32
	mixFrame      []float32
	pcm           []byte
	// audioErr keeps the first audio write failure. The clock goroutine has
	// nowhere to report one, so the failure is held here and surfaced by Finish.
	audioErr error

	// videoMu guards the video file, which is still written straight from the
	// media goroutine. Video is stored verbatim: it has no timeline to hold and
	// no second track to stay aligned with, so it has nothing to gain from the
	// clock.
	videoMu     sync.Mutex
	videoFile   *os.File
	videoLen    int
	videoErr    error
	videoClosed bool
}

// NewRecorder prepares a recorder for one call. No file is created, and no
// goroutine started, until the first frame arrives.
func NewRecorder(tmpDir, channelID, callID string) *Recorder {
	return newRecorder(tmpDir, channelID, callID, frameInterval)
}

// newRecorder is NewRecorder with the clock's interval injected, so a test can
// drive the frame grid itself instead of waiting on real time.
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

// AudioKey is where this call's mixed recording is stored. It stays the plain
// key it has always been: it is the one the chat's player points at.
func (r *Recorder) AudioKey() string {
	return fmt.Sprintf("calls/%s/%s.wav", r.channelID, r.callID)
}

// PeerAudioKey is where the customer's own track is stored.
func (r *Recorder) PeerAudioKey() string {
	return fmt.Sprintf("calls/%s/%s-peer.wav", r.channelID, r.callID)
}

// OperatorAudioKey is where the operator's own track is stored.
func (r *Recorder) OperatorAudioKey() string {
	return fmt.Sprintf("calls/%s/%s-operator.wav", r.channelID, r.callID)
}

// VideoKey is where this call's video recording is stored.
func (r *Recorder) VideoKey() string {
	return fmt.Sprintf("calls/%s/%s.h264", r.channelID, r.callID)
}

// WritePeerAudio takes one decoded mono frame from the customer. It only ever
// queues: it is called from the calling library's media goroutine, so it must
// return in memcpy time and must never report an error there -- a failed
// recording must not tear down a live call.
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

// WriteOperatorAudio takes one s16le mono frame of what the gateway is
// transmitting, tapped from the call's single outbound source -- see
// outbound.go. Everything the peer heard is on this track, the play command's
// announcements included, because that is what "the operator's side of the
// call" means on a record.
//
// The caller keeps ownership of frame: it is decoded here and never retained.
// Like WritePeerAudio, this runs on a media goroutine and only ever queues.
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

// startLocked starts the clock on the first frame from either side.
//
// Starting lazily is what keeps the promise that a call with no audio leaves no
// object behind: a ticker running from the moment the call was tracked would
// fill three files with silence for a call nobody ever spoke a word on.
func (r *Recorder) startLocked() {
	if r.started {
		return
	}
	r.started = true
	r.stop = make(chan struct{})
	r.done = make(chan struct{})
	go r.run()
}

// run is the recorder's clock.
func (r *Recorder) run() {
	defer close(r.done)

	ticker := time.NewTicker(r.interval)
	// Stopped here, on the goroutine that owns it, rather than left to the
	// call's teardown: one leaked ticker per call is the kind of leak that only
	// shows up under load.
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

// drain writes out whatever the rings still hold when the call ends, so a last
// word spoken as the peer hung up lands on the recording instead of being
// dropped with the buffer. The rings are bounded, so this is too.
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

// writeFrame writes one tick: one frame of each side, and their sum.
//
// The two rings are drained under mu and the three file writes deliberately are
// not -- see the mu comment on Recorder. Both sides are taken in the same
// critical section so a frame arriving between them cannot land on one track's
// tick and the next tick of the other.
func (r *Recorder) writeFrame() {
	if r.audioErr != nil {
		return
	}

	r.mu.Lock()
	r.peer.take(r.peerFrame)
	r.operator.take(r.operatorFrame)
	r.mu.Unlock()

	// The mix is the plain sum of the two sides, clamped on the way to s16 --
	// never halved. Only one side is talking for the majority of any call, and
	// halving would gut the volume for all of it to buy headroom for the
	// moments where both talk at once. Clamping pays for those moments only,
	// and only when both are loud.
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

// writeTrack appends one frame to a track as s16le PCM.
func (r *Recorder) writeTrack(f *os.File, frame []float32) error {
	for i, sample := range frame {
		binary.LittleEndian.PutUint16(r.pcm[i*2:], uint16(pcmS16(sample)))
	}
	if _, err := f.Write(r.pcm); err != nil {
		return fmt.Errorf("call: write audio frame: %w", err)
	}
	return nil
}

// openTracks creates the three audio temp files on the first tick. They are
// created together because they are one recording: a run that can only produce
// some of them has nothing useful to leave behind.
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
	// Placeholder header; Finish rewrites it with the real size.
	if _, err := f.Write(make([]byte, wavHeaderSize)); err != nil {
		cleanup(f)
		return nil, fmt.Errorf("call: write audio header: %w", err)
	}
	return f, nil
}

// pcmS16 converts one float sample to signed 16-bit, clamping out-of-range
// input. Without the clamp an overshooting sample -- which the mix produces by
// design whenever both sides are loud at once -- wraps into the opposite sign
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

// Finish stops the clock, closes the temp files, uploads whatever was captured
// and reports the resulting media descriptors. Any of them may be nil when
// nothing was captured.
//
// The temp files are always removed, upload error or not: a gateway that keeps
// running has to not accumulate dead recordings on disk.
func (r *Recorder) Finish(ctx context.Context, store RecordingStore) (Recording, error) {
	r.mu.Lock()
	if r.closing {
		r.mu.Unlock()
		return Recording{}, nil
	}
	r.closing = true
	stop, done := r.stop, r.done
	r.mu.Unlock()

	// Stopping the clock and joining it before touching a file is what lets the
	// files be lock-free: past this point nothing else writes them.
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

	// All three tracks are written on the same tick, so they are the same
	// length by construction and one size covers them.
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
	// dataLen counts only ticks that made it onto all three tracks, so a write
	// that failed part way through a tick can leave one track a frame longer
	// than the header is about to declare. Cut it back rather than upload a wav
	// whose data chunk disagrees with its own size.
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

// audioRing holds one side's samples between arriving and being written.
//
// It buffers samples rather than whole frames on purpose: a producer that does
// not hand over exactly FrameSamples at a time -- a half-filled outbound frame,
// a test feeding three samples -- still lands on the recorder's frame grid
// instead of being stretched to a full frame of its own.
//
// It is not safe on its own; the Recorder's mu is what guards it.
type audioRing struct {
	samples []float32
}

// write queues decoded samples.
func (r *audioRing) write(frame []float32) {
	r.samples = append(r.samples, frame...)
	r.trim()
}

// write16 queues s16le samples, decoding them here so the tick has nothing to
// do but sum and clamp.
func (r *audioRing) write16(frame []byte) {
	for i := 0; i+1 < len(frame); i += 2 {
		sample := int16(binary.LittleEndian.Uint16(frame[i:]))
		r.samples = append(r.samples, float32(sample)/32767)
	}
	r.trim()
}

// trim enforces the bound, dropping the oldest samples. See
// recorderRingSamples for why the oldest.
func (r *audioRing) trim() {
	if over := len(r.samples) - recorderRingSamples; over > 0 {
		r.samples = append(r.samples[:0], r.samples[over:]...)
	}
}

// take fills dst with the next frame, padding with silence when the ring runs
// dry. The silence is the point rather than a fallback: a side that has sent
// nothing since the last tick contributes exactly one frame of nothing, which
// is what keeps the three tracks on one timeline.
func (r *audioRing) take(dst []float32) {
	n := copy(dst, r.samples)
	r.samples = append(r.samples[:0], r.samples[n:]...)
	clear(dst[n:])
}

func (r *audioRing) empty() bool { return len(r.samples) == 0 }

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
