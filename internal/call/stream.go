package call

import (
	"fmt"
	"io"
	"sync"
)

// streamAudioBuffer and streamVideoBuffer size the outbound channels a
// connected operator drains. Sized generously enough to absorb a brief stall
// (a slow websocket write, a GC pause) without dropping, but bounded: an
// operator that never catches up must lose frames, not turn into unbounded
// memory growth.
//
// operatorAudioBuffer sizes the queue feeding Play in the other direction.
// Play's own consumption sets the real pace (it plays PCM out at 16 kHz,
// same as it came in); the buffer only has to absorb the gap between an
// operator's send and Play's next read, not the whole call.
const (
	streamAudioBuffer   = 50
	streamVideoBuffer   = 50
	operatorAudioBuffer = 50
)

// Stream carries one call's media to and from a connected operator: the
// peer's audio and video flow out on buffered channels, and the operator's
// own audio and video flow back into the call.
//
// receiveAudio and receiveVideo run on the calling library's media goroutine,
// the same one that feeds the Recorder. The outbound channels are buffered
// and drop on overflow for exactly that reason: a slow or gone operator must
// lose frames, never stall the goroutine the recording depends on.
type Stream struct {
	lc LiveCall

	audio    chan []float32
	video    chan []byte
	keyframe chan struct{}

	// operatorAudio queues frames from WriteAudio for the feeder goroutine
	// below. It sits between the operator and the pipe so a WriteAudio call
	// never blocks on Play actually consuming: it only ever queues onto a
	// channel, never writes into the io.Pipe directly.
	operatorAudio chan []byte
	pipeR         *io.PipeReader
	pipeW         *io.PipeWriter

	// mu guards closed and, by extension, every channel above: a send must
	// never race a close of the same channel. Held for read while a channel
	// send is in flight (WriteAudio, receiveAudio, receiveVideo) and for
	// write only by close, so ordinary traffic never contends with itself,
	// only with teardown. WriteVideo takes it only to snapshot closed, never
	// across the call into the library -- see the comment there.
	mu     sync.RWMutex
	closed bool
}

// newStream builds a stream over a live call and wires the operator's audio
// path immediately: Play is only ever handed one reader per call, so the
// pipe is created up front rather than lazily on the first WriteAudio.
func newStream(lc LiveCall) *Stream {
	pr, pw := io.Pipe()
	s := &Stream{
		lc:            lc,
		audio:         make(chan []float32, streamAudioBuffer),
		video:         make(chan []byte, streamVideoBuffer),
		keyframe:      make(chan struct{}, 1),
		operatorAudio: make(chan []byte, operatorAudioBuffer),
		pipeR:         pr,
		pipeW:         pw,
	}

	// Feeds the pipe from operatorAudio so WriteAudio only ever queues. This
	// goroutine, unlike WriteAudio's caller, is not on the media path, so it
	// is fine for it to block on the pipe write until Play drains it.
	go func() {
		for frame := range s.operatorAudio {
			if _, err := pw.Write(frame); err != nil {
				return
			}
		}
	}()

	// Play blocks for the life of the call, reading whatever the feeder
	// above writes; it must run off the caller's goroutine.
	go func() {
		if err := lc.Play(pr); err != nil {
			_ = pr.CloseWithError(err)
		}
	}()

	// A stream attached mid-call has missed every keyframe so far: H.264
	// access units between here and the peer's next one are undecodable on
	// their own, so the consumer is told up front to wait for it.
	s.requestKeyframe()

	return s
}

// Audio receives the peer's decoded frames while the stream is attached.
func (s *Stream) Audio() <-chan []float32 { return s.audio }

// Video receives the peer's H.264 Annex-B access units while the stream is
// attached.
func (s *Stream) Video() <-chan []byte { return s.video }

// Keyframe signals that the peer's video needs a fresh keyframe, e.g. when an
// operator's viewer just joined mid-call. It is a signal, not a queue: only
// whether one is pending matters, so the channel holds at most one.
func (s *Stream) Keyframe() <-chan struct{} { return s.keyframe }

// WriteAudio queues one s16le mono frame from the operator into the call.
// It only ever queues the frame onto a buffered channel; a background
// goroutine carries it to Play's reader, so a stalled Play never blocks the
// caller here. The buffer itself drops rather than blocks when full, for the
// same reason: an operator sending faster than the call plays must not stall
// whatever is calling WriteAudio.
func (s *Stream) WriteAudio(frame []byte) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return fmt.Errorf("call: write operator audio: %w", io.ErrClosedPipe)
	}

	select {
	case s.operatorAudio <- append([]byte(nil), frame...):
		return nil
	default:
		return fmt.Errorf("call: write operator audio: buffer full, operator is sending faster than the call plays")
	}
}

// WriteVideo sends one Annex-B access unit from the operator straight
// through to the call; the gateway never re-encodes video.
//
// SendVideo touches no channel of ours, so it needs no fence against close:
// the lock here only snapshots closed and is released before the call.
// SendVideo carries no bounded-time contract (a contended send lock or a
// full socket buffer in the real implementation can stall it), and holding
// mu across it would queue every other sender behind a pending Lock in
// close -- including receiveAudio and receiveVideo on the media goroutine,
// which is exactly what this package exists to keep from stalling.
func (s *Stream) WriteVideo(accessUnit []byte) error {
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return fmt.Errorf("call: write operator video: %w", io.ErrClosedPipe)
	}

	if err := s.lc.SendVideo(accessUnit, 0); err != nil {
		return fmt.Errorf("call: write operator video: %w", err)
	}
	return nil
}

// receiveAudio delivers one peer frame, dropping it if the operator isn't
// keeping up. Called from the library's media goroutine.
func (s *Stream) receiveAudio(frame []float32) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return
	}

	select {
	case s.audio <- frame:
	default:
	}
}

// receiveVideo delivers one peer access unit, dropping it if the operator
// isn't keeping up. Called from the library's media goroutine.
func (s *Stream) receiveVideo(accessUnit []byte) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return
	}

	select {
	case s.video <- accessUnit:
	default:
	}
}

// requestKeyframe marks a keyframe as pending, coalescing repeated requests.
// Only called during construction, before the stream is reachable from
// anywhere else, so it needs no locking of its own.
func (s *Stream) requestKeyframe() {
	select {
	case s.keyframe <- struct{}{}:
	default:
	}
}

// close tears the stream down. It is idempotent, since detach may run more
// than once (e.g. a caller's defer alongside a replacing AttachStream).
//
// closed flips under the write lock first, which fences every sender above:
// none can be mid-send past that point, so every channel below is safe to
// close. Closing pipeR then unblocks the feeder goroutine if it is stuck
// writing into a Play that never reads; closing pipeW signals Play's reader
// with EOF so a real implementation's read loop returns.
func (s *Stream) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.mu.Unlock()

	close(s.operatorAudio)
	_ = s.pipeR.Close()
	_ = s.pipeW.Close()
	close(s.audio)
	close(s.video)
	close(s.keyframe)
}

// AttachStream registers a stream on a tracked call, returning a detach
// func. Only one stream at a time: a second attach replaces the first,
// closing it, so a reconnecting operator never leaves two consumers racing
// for the same frames.
func (m *Manager) AttachStream(channelID, callID string) (*Stream, func(), bool) {
	t, ok := m.registry.Get(channelID, callID)
	if !ok {
		return nil, func() {}, false
	}

	stream := newStream(t.Live)

	if old := t.setStream(stream); old != nil {
		old.close()
	}

	detach := func() {
		t.clearStream(stream)
		stream.close()
	}

	return stream, detach, true
}
