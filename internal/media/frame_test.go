package media_test

import (
	"bytes"
	"testing"

	"github.com/w3nder/whatsmeow-gateway/internal/media"
)

func TestFrameRoundTrip(t *testing.T) {
	payload := []byte{0x01, 0x02, 0x03}
	kind, got, err := media.DecodeFrame(media.EncodeFrame(media.FrameVideo, payload))
	if err != nil {
		t.Fatalf("DecodeFrame: %v", err)
	}
	if kind != media.FrameVideo || !bytes.Equal(got, payload) {
		t.Errorf("kind=%x payload=% x, want %x % x", kind, got, media.FrameVideo, payload)
	}
}

func TestDecodeFrameRejectsAnEmptyFrame(t *testing.T) {
	if _, _, err := media.DecodeFrame(nil); err == nil {
		t.Fatal("err = nil, want a rejection for an empty frame")
	}
}

func TestEncodeFrameCarriesAnEmptyPayload(t *testing.T) {
	kind, payload, err := media.DecodeFrame(media.EncodeFrame(media.FrameKeyframe, nil))
	if err != nil {
		t.Fatalf("DecodeFrame: %v", err)
	}
	if kind != media.FrameKeyframe || len(payload) != 0 {
		t.Errorf("kind=%x len=%d, want keyframe with no payload", kind, len(payload))
	}
}
