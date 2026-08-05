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
const (
	streamAudioBuffer = 50
	streamVideoBuffer = 50
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

	// out is the call's own outbound audio source, shared with the play
	// command and owned by the Tracked call rather than by this stream. The
	// stream is one producer into it, not its owner: a detaching operator
	// must not take the call's transmit path down with it.
	out *outboundAudio

	// mu guards closed and, by extension, every channel above: a send must
	// never race a close of the same channel. Held for read while a channel
	// send is in flight (receiveAudio, receiveVideo, WriteAudio) and for write
	// only by close, so ordinary traffic never contends with itself, only with
	// teardown. WriteVideo takes it only to snapshot closed, never across the
	// call into the library -- see the comment there.
	mu     sync.RWMutex
	closed bool
}

// newStream builds a stream over a live call, writing the operator's audio
// into the call's shared outbound source. Nothing here subscribes a player:
// Manager.Track already handed that source to Play once, for the whole life
// of the call, so attaching and detaching an operator never disturbs what the
// gateway is transmitting.
func newStream(lc LiveCall, out *outboundAudio) *Stream {
	s := &Stream{
		lc:       lc,
		out:      out,
		audio:    make(chan []float32, streamAudioBuffer),
		video:    make(chan []byte, streamVideoBuffer),
		keyframe: make(chan struct{}, 1),
	}

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

// WriteAudio queues one s16le mono frame from the operator into the call. It
// only ever queues: the call's transmit loop picks the frame up on its own
// cadence, so nothing about how fast (or whether) the call is draining can
// stall the caller here. An operator running ahead of the call loses its
// oldest queued frames rather than blocking -- see operatorQueueFrames.
//
// The frame is copied because the caller owns the buffer it read the
// websocket message into and is free to reuse it the moment this returns.
//
// mu is held across writeOperator, unlike WriteVideo below, and that is safe
// precisely because writeOperator has a bounded-time contract: it takes one
// mutex that is only ever held for a memcpy and can never wait on the call.
// Holding it here is the fence that lets detach flush the call's queue and
// know no frame from the operator it just dropped can still arrive.
func (s *Stream) WriteAudio(frame []byte) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return fmt.Errorf("call: write operator audio: %w", io.ErrClosedPipe)
	}

	if err := s.out.writeOperator(append([]byte(nil), frame...)); err != nil {
		return fmt.Errorf("call: write operator audio: %w", err)
	}
	return nil
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
// Two callers share it, and both mean the same thing downstream -- "the
// operator's encoder needs to emit an IDR now" -- so they share the channel
// rather than getting one each: newStream, once, for an operator attaching
// mid-call (it has missed every keyframe so far); and the peer's own
// OnVideoKeyframeRequest callback, any number of times, after packet loss.
// The construction-time caller predates any other goroutine reaching this
// stream and needs no lock, but the peer callback runs on the calling
// library's own goroutine for the life of the call, arbitrarily late, so it
// has to be fenced against close the same way receiveAudio/receiveVideo are.
func (s *Stream) requestKeyframe() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return
	}

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
// close. The call's outbound audio source is deliberately left alone: it
// belongs to the call, not to the operator, and a stream being closed says
// nothing about whether some other operator now owns it. Flushing it is the
// detach path's job -- see AttachStream.
func (s *Stream) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.mu.Unlock()

	close(s.audio)
	close(s.video)
	close(s.keyframe)
}

// AttachStream registers a stream on a tracked call, returning a detach
// func. Only one stream at a time: a second attach replaces the first,
// closing it, so a reconnecting operator never leaves two consumers racing
// for the same frames.
//
// It reports false both for a call that was never live and for one that ended
// between the lookup and the attach. The second case is a real race -- an
// operator clicking "listen" as the peer hangs up -- and it must not leave a
// stream registered on a call that has already been finished: nothing would
// ever close it, so the operator's socket would wait forever for the end frame
// that only a closed Audio() produces.
func (m *Manager) AttachStream(channelID, callID string) (*Stream, func(), bool) {
	t, ok := m.registry.Get(channelID, callID)
	if !ok {
		return nil, func() {}, false
	}

	stream := newStream(t.Live, t.outbound)

	old, attached := t.setStream(stream)
	if !attached {
		// The call was torn down while this stream was being built. Close it
		// here rather than handing back something the caller would park on.
		stream.close()
		return nil, func() {}, false
	}
	if old != nil {
		old.close()
	}

	detach := func() {
		// close first: it is what fences WriteAudio, so no frame from this
		// operator can land in the call's queue after the flush below.
		stream.close()

		// Only an operator that is genuinely still the attached one may flush
		// the call's queued outbound audio. A stale detach -- a caller's defer
		// running after AttachStream already replaced this stream -- must not
		// wipe the audio its replacement has queued.
		if t.clearStream(stream) {
			t.outbound.clearOperator()
		}
	}

	return stream, detach, true
}
