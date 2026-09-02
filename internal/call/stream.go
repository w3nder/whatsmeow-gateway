package call

import (
	"fmt"
	"io"
	"sync"
)

const (
	streamAudioBuffer = 50
	streamVideoBuffer = 50
)

type Stream struct {
	lc LiveCall

	audio    chan []float32
	video    chan []byte
	keyframe chan struct{}

	out *outboundAudio

	mu     sync.RWMutex
	closed bool
}

func newStream(lc LiveCall, out *outboundAudio) *Stream {
	s := &Stream{
		lc:       lc,
		out:      out,
		audio:    make(chan []float32, streamAudioBuffer),
		video:    make(chan []byte, streamVideoBuffer),
		keyframe: make(chan struct{}, 1),
	}

	s.requestKeyframe()

	return s
}

func (s *Stream) Audio() <-chan []float32 { return s.audio }

func (s *Stream) Video() <-chan []byte { return s.video }

func (s *Stream) Keyframe() <-chan struct{} { return s.keyframe }

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

func (m *Manager) AttachStream(channelID, callID string) (*Stream, func(), bool) {
	t, ok := m.registry.Get(channelID, callID)
	if !ok {
		return nil, func() {}, false
	}

	stream := newStream(t.Live, t.outbound)

	old, attached := t.setStream(stream)
	if !attached {
		stream.close()
		return nil, func() {}, false
	}
	if old != nil {
		old.close()
	}

	detach := func() {
		stream.close()

		if t.clearStream(stream) {
			t.outbound.clearOperator()
		}
	}

	return stream, detach, true
}
