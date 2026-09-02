package call

import "testing"

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

	second := newStream(nil, tracked.outbound)
	if _, ok := tracked.setStream(second); ok {
		t.Error("setStream accepted a stream after the call was finished")
	}
	if tracked.currentStream() != nil {
		t.Error("a stream is attached to a finished call")
	}
}

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
