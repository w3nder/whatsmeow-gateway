package call

import (
	"fmt"
	"io"
	"sync"
)

const operatorQueueFrames = 50

type outboundAudio struct {
	recorder *Recorder

	mu     sync.Mutex
	closed bool

	announcement []byte

	queue   [][]byte
	pending []byte
}

func newOutboundAudio(recorder *Recorder) *outboundAudio {
	return &outboundAudio{recorder: recorder}
}

func (o *outboundAudio) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	o.mu.Lock()

	if o.closed {
		o.mu.Unlock()
		return 0, io.EOF
	}

	n := 0
	switch {
	case len(o.announcement) > 0:
		n = copy(p, o.announcement)
		o.announcement = o.announcement[n:]
	default:
		for n < len(p) {
			if len(o.pending) == 0 {
				if len(o.queue) == 0 {
					break
				}
				o.pending = o.queue[0]
				o.queue[0] = nil
				o.queue = o.queue[1:]
			}
			copied := copy(p[n:], o.pending)
			o.pending = o.pending[copied:]
			n += copied
		}
	}

	clear(p[n:])
	recorder := o.recorder
	o.mu.Unlock()

	if recorder != nil && n > 0 {
		recorder.WriteOperatorAudio(p)
	}
	return len(p), nil
}

func (o *outboundAudio) Close() error {
	o.mu.Lock()
	o.closed = true
	o.announcement, o.queue, o.pending = nil, nil, nil
	o.mu.Unlock()
	return nil
}

func (o *outboundAudio) writeOperator(frame []byte) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return fmt.Errorf("call: queue operator audio: %w", io.ErrClosedPipe)
	}
	if len(o.announcement) > 0 {
		return nil
	}

	if len(o.queue) >= operatorQueueFrames {
		copy(o.queue, o.queue[1:])
		o.queue = o.queue[:len(o.queue)-1]
	}
	o.queue = append(o.queue, frame)
	return nil
}

func (o *outboundAudio) clearOperator() {
	o.mu.Lock()
	o.queue, o.pending = nil, nil
	o.mu.Unlock()
}

func (o *outboundAudio) playAnnouncement(pcm []byte) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return fmt.Errorf("call: play announcement: %w", io.ErrClosedPipe)
	}

	o.announcement = pcm
	o.queue, o.pending = nil, nil
	return nil
}

var _ io.ReadCloser = (*outboundAudio)(nil)
