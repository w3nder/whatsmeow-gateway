package groups

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestToJPEGWaitsForAFreeSlot(t *testing.T) {
	for range photoSlotCount {
		photoSlots <- struct{}{}
	}
	defer func() {
		for range photoSlotCount {
			<-photoSlots
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := ToJPEG(ctx, []byte("anything")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("with every slot taken the conversion must wait and give up with its context, got %v", err)
	}
}

func TestExifOrientationIgnoresMalformedSegments(t *testing.T) {
	for _, data := range [][]byte{nil, {0xFF, 0xD8}, {0xFF, 0xD8, 0xFF, 0xE1, 0x00, 0x01}, {0xFF, 0xD8, 0xFF, 0xE1, 0xFF, 0xFF}} {
		if got := exifOrientation(data); got != orientationNormal {
			t.Fatalf("malformed %v → %d, want normal", data, got)
		}
	}
}
