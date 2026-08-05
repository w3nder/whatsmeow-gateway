package call

import (
	"fmt"
	"io"
	"sync"
)

// operatorQueueFrames bounds how much operator audio may wait to be sent.
// Generous enough to absorb a brief stall (a slow websocket, a GC pause)
// without losing anything, but bounded: an operator sending faster than the
// call drains must lose the oldest frames rather than grow memory without
// limit. Live audio is worth nothing once it is late, so the oldest frame is
// what gets dropped, never the newest.
const operatorQueueFrames = 50

// outboundAudio is the single source of everything the gateway sends to the
// peer, and it is the one object the calling library's audio transmit loop
// reads from. It exists because that loop is synchronous.
//
// The library's send loop is frame-paced from the moment the relay connects and
// is deliberately NOT gated on whether anything has audio to send: WhatsApp's
// relay learns our SSRC from our first RTP packet and refuses to bridge the
// peer's media back until it has seen our stream, so the loop must emit a
// packet every 60 ms even when there is nothing to say. It gets that packet by
// calling AudioSource.ReadFrame on its own goroutine, with nothing between the
// two. A source that blocks does not merely delay our own audio -- it stops the
// gateway emitting RTP at all, which stops the relay bridging the peer back,
// which flatlines the recording.
//
// So Read here never blocks and never fails while the call is up: it returns
// whatever is queued, padded with (or replaced entirely by) silence. An
// operator who attaches and says nothing -- listening in, microphone muted, a
// viewer that only renders video -- is the common case, and it must look
// exactly like a call with no operator at all.
//
// One of these is created per call and handed to Play exactly once, at Track
// time. That is also what makes the player slot unambiguous: Play is not
// additive in the library (each call builds a fresh Player and replaces the
// previous one, dropping it without stopping it), so more than one caller
// means whoever plays last silently orphans everyone else. Every producer --
// the operator's microphone and the play command's announcement -- goes
// through this one source instead.
type outboundAudio struct {
	// mu is held only for the memcpy-sized critical sections below, never
	// across anything that can wait, so neither the library's send loop nor
	// the operator's websocket goroutine can be delayed by the other.
	mu     sync.Mutex
	closed bool

	// announcement is the remaining bytes of a play command. It has the floor
	// while it lasts: see writeOperator.
	announcement []byte

	// queue holds whole operator frames as WriteAudio queued them; pending is
	// the partially-consumed head of that queue, since one Read rarely lands
	// on a frame boundary.
	queue   [][]byte
	pending []byte
}

func newOutboundAudio() *outboundAudio {
	return &outboundAudio{}
}

// Read fills p with the next outbound audio, in priority order: an
// announcement if one is playing, else the operator's queued microphone, else
// silence. It always fills p completely and never returns an error until the
// call is over, so the library's send loop always has a frame within
// nanoseconds -- see the type comment for why that is not negotiable.
//
// Reads are paced by the caller, not by us: the send loop asks for exactly one
// 60 ms frame every 60 ms, so returning immediately here does not run an
// announcement out at wire speed.
func (o *outboundAudio) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	// EOF is the only way to tell the library's Player that this source is
	// finished; it then goes idle and the send loop falls back to its own
	// silence, which is exactly right for a call that is over.
	if o.closed {
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

	// Whatever the producers did not fill is silence, not a short read: a
	// short read would send the library's io.ReadFull back around for more,
	// and with nothing queued it would block there.
	clear(p[n:])
	return len(p), nil
}

// Close ends the source. The library's Player closes it on EOF or Stop, and
// the call's teardown closes it directly; it is idempotent either way.
func (o *outboundAudio) Close() error {
	o.mu.Lock()
	o.closed = true
	o.announcement, o.queue, o.pending = nil, nil, nil
	o.mu.Unlock()
	return nil
}

// writeOperator queues one s16le frame from the operator's microphone.
//
// A frame arriving while an announcement is playing is discarded rather than
// queued. That is the policy, stated once here: an announcement (an IVR
// prompt, a hold message) takes the floor for its duration, and the operator's
// microphone resumes the instant it is exhausted. Queueing instead would play
// the operator's words back seconds late, which is worse than dropping them --
// this is live audio, and a frame is worthless once its moment has passed.
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

// clearOperator drops whatever the operator queued, without touching a playing
// announcement. Called when a stream detaches: the next operator to attach must
// not open with the previous one's leftover audio.
func (o *outboundAudio) clearOperator() {
	o.mu.Lock()
	o.queue, o.pending = nil, nil
	o.mu.Unlock()
}

// playAnnouncement starts pcm playing to the peer, replacing any announcement
// still in flight and any operator audio queued behind it. See writeOperator
// for the precedence rule.
//
// It returns once the audio is queued, not once it has played: the send loop
// drains it at 60 ms per frame and nothing here waits for that.
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
