package call_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/call"
)

// A decoder handed an IDR without its parameter sets cannot start, so SPS and
// PPS must stay attached to the picture that follows them.
func TestSplitAnnexBKeepsParameterSetsWithTheirPicture(t *testing.T) {
	stream := []byte{
		0, 0, 0, 1, 0x67, 0xAA, // SPS
		0, 0, 0, 1, 0x68, 0xBB, // PPS
		0, 0, 0, 1, 0x65, 0xCC, // IDR
		0, 0, 0, 1, 0x41, 0xDD, // non-IDR slice
	}
	units := call.SplitAnnexB(stream)
	if len(units) != 2 {
		t.Fatalf("got %d access units, want 2 (SPS+PPS+IDR together, then the P slice)", len(units))
	}
	wantFirst := []byte{
		0, 0, 0, 1, 0x67, 0xAA,
		0, 0, 0, 1, 0x68, 0xBB,
		0, 0, 0, 1, 0x65, 0xCC,
	}
	if !bytes.Equal(units[0], wantFirst) {
		t.Errorf("unit[0] = % x, want % x", units[0], wantFirst)
	}
	wantSecond := []byte{0, 0, 0, 1, 0x41, 0xDD}
	if !bytes.Equal(units[1], wantSecond) {
		t.Errorf("unit[1] = % x, want % x", units[1], wantSecond)
	}
}

func TestSplitAnnexBOnThreeByteStartCodes(t *testing.T) {
	stream := []byte{
		0, 0, 1, 0x65, 0xAA,
		0, 0, 1, 0x41, 0xBB,
	}
	units := call.SplitAnnexB(stream)
	if len(units) != 2 {
		t.Fatalf("got %d access units, want 2", len(units))
	}
	if !bytes.Equal(units[0], []byte{0, 0, 1, 0x65, 0xAA}) {
		t.Errorf("unit[0] = % x", units[0])
	}
	if !bytes.Equal(units[1], []byte{0, 0, 1, 0x41, 0xBB}) {
		t.Errorf("unit[1] = % x", units[1])
	}
}

// An access-unit delimiter opens the next frame.
func TestSplitAnnexBSplitsOnAccessUnitDelimiter(t *testing.T) {
	stream := []byte{
		0, 0, 0, 1, 0x09, 0xF0, // AUD
		0, 0, 0, 1, 0x65, 0xAA, // IDR
		0, 0, 0, 1, 0x09, 0xF0, // AUD
		0, 0, 0, 1, 0x41, 0xBB, // slice
	}
	units := call.SplitAnnexB(stream)
	if len(units) != 2 {
		t.Fatalf("got %d access units, want 2", len(units))
	}
}

func TestSplitAnnexBRejectsGarbage(t *testing.T) {
	if units := call.SplitAnnexB([]byte{1, 2, 3, 4}); len(units) != 0 {
		t.Errorf("got %d units from a stream with no start code, want 0", len(units))
	}
	if units := call.SplitAnnexB(nil); len(units) != 0 {
		t.Errorf("got %d units from nil, want 0", len(units))
	}
}

// The whole stream must survive the split: no byte dropped, none duplicated.
func TestSplitAnnexBIsLossless(t *testing.T) {
	stream := []byte{
		0, 0, 0, 1, 0x67, 0xAA,
		0, 0, 0, 1, 0x65, 0xCC, 0xDD, 0xEE,
		0, 0, 1, 0x41, 0xFF,
	}
	var rejoined []byte
	for _, unit := range call.SplitAnnexB(stream) {
		rejoined = append(rejoined, unit...)
	}
	if !bytes.Equal(rejoined, stream) {
		t.Errorf("rejoined = % x, want % x", rejoined, stream)
	}
}

func TestPlayAnnexBSendsEveryUnit(t *testing.T) {
	lc := &fakeCall{id: "C1"}
	units := [][]byte{{0, 0, 0, 1, 0x65}, {0, 0, 0, 1, 0x41}}

	if err := call.PlayAnnexB(context.Background(), lc, units, 1000); err != nil {
		t.Fatalf("PlayAnnexB: %v", err)
	}
	sent := lc.sentVideo()
	if len(sent) != 2 {
		t.Fatalf("sent %d units, want 2", len(sent))
	}
	if !bytes.Equal(sent[0], units[0]) {
		t.Errorf("unit[0] = % x, want % x", sent[0], units[0])
	}
}

func TestPlayAnnexBStopsOnContextCancel(t *testing.T) {
	lc := &fakeCall{id: "C1"}
	units := make([][]byte, 100)
	for i := range units {
		units[i] = []byte{0, 0, 0, 1, 0x41}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := call.PlayAnnexB(ctx, lc, units, 30); err == nil {
		t.Fatal("PlayAnnexB err = nil, want the context error")
	}
	if got := len(lc.sentVideo()); got >= len(units) {
		t.Errorf("sent %d units after cancellation, want fewer than %d", got, len(units))
	}
}

func TestDispatchVideoPlayStreamsTheFile(t *testing.T) {
	pub := &memPublisher{}
	m := newTestManager(t, pub, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)
	lc := &fakeCall{id: "C1"}
	caller.fireIncoming(lc)

	fetch := func(context.Context, string) ([]byte, error) {
		return []byte{0, 0, 0, 1, 0x65, 0xAA, 0, 0, 0, 1, 0x41, 0xBB}, nil
	}
	if err := m.Dispatch(context.Background(), caller, amqp.GatewayCallCommand{
		ChannelID: "chan-a", CallID: "C1", CommandID: "cmd-1",
		Action: "video.play", MediaURL: "https://example.test/clip.h264",
	}, fetch); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if acks := pub.typed(call.EventCommandAck); len(acks) != 1 {
		t.Errorf("acks = %+v, want one", acks)
	}
	if failed := pub.typed(call.EventCommandFailed); len(failed) != 0 {
		t.Errorf("failures = %+v, want none", failed)
	}
}

func TestDispatchVideoPlayRejectsAStreamWithNoAccessUnit(t *testing.T) {
	pub := &memPublisher{}
	m := newTestManager(t, pub, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)
	caller.fireIncoming(&fakeCall{id: "C1"})

	fetch := func(context.Context, string) ([]byte, error) { return []byte("not h264"), nil }
	if err := m.Dispatch(context.Background(), caller, amqp.GatewayCallCommand{
		ChannelID: "chan-a", CallID: "C1", CommandID: "cmd-1",
		Action: "video.play", MediaURL: "https://example.test/bad.bin",
	}, fetch); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	failed := pub.typed(call.EventCommandFailed)
	if len(failed) != 1 || failed[0].Error.Code != call.CodeMediaFetch {
		t.Errorf("failures = %+v, want one media_fetch_failed", failed)
	}
}
