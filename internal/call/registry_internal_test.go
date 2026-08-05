package call

import "testing"

// The attach/teardown race in AttachStream is decided inside Tracked, and the
// losing interleaving cannot be forced from outside the package: by the time
// AttachStream is observable the call is already out of the registry. These
// tests write the interleaving out by hand instead.

func TestSetStreamRefusesOnceTheCallIsFinished(t *testing.T) {
	tracked := &Tracked{outbound: newOutboundAudio(nil)}
	first := newStream(nil, tracked.outbound)

	old, ok := tracked.setStream(first)
	if !ok {
		t.Fatal("setStream refused the first stream on a live call")
	}
	if old != nil {
		t.Errorf("setStream returned %v as the previous stream, want nil", old)
	}

	if got := tracked.finishStream(); got != first {
		t.Errorf("finishStream returned %v, want the attached stream", got)
	}

	// This is the losing side of the race: AttachStream had already passed
	// the registry check when teardown ran, and is only now storing its
	// stream. Accepting it would strand the operator on a call that is gone.
	second := newStream(nil, tracked.outbound)
	if _, ok := tracked.setStream(second); ok {
		t.Error("setStream accepted a stream after the call was finished")
	}
	if tracked.currentStream() != nil {
		t.Error("a stream is attached to a finished call")
	}
}

// finishStream runs from endOnce, but AbortChannel and the library's own end
// callback can still both reach a call, so it has to survive being called
// twice.
func TestFinishStreamIsIdempotent(t *testing.T) {
	tracked := &Tracked{outbound: newOutboundAudio(nil)}
	stream := newStream(nil, tracked.outbound)
	if _, ok := tracked.setStream(stream); !ok {
		t.Fatal("setStream refused a stream on a live call")
	}

	if got := tracked.finishStream(); got != stream {
		t.Errorf("first finishStream returned %v, want the attached stream", got)
	}
	if got := tracked.finishStream(); got != nil {
		t.Errorf("second finishStream returned %v, want nil", got)
	}
}
