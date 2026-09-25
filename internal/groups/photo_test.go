package groups_test

import (
	"bytes"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"testing"

	"github.com/w3nder/whatsmeow-gateway/internal/groups"
)

func TestToJPEGConvertsPNG(t *testing.T) {
	img := image.NewRGBA(image.Rect(0, 0, 4, 4))
	img.Set(1, 1, color.RGBA{R: 255, A: 255})
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatal(err)
	}

	out, err := groups.ToJPEG(buf.Bytes())
	if err != nil {
		t.Fatalf("ToJPEG: %v", err)
	}
	if _, err := jpeg.Decode(bytes.NewReader(out)); err != nil {
		t.Fatalf("output is not jpeg: %v", err)
	}
}

func TestToJPEGKeepsJPEG(t *testing.T) {
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, image.NewRGBA(image.Rect(0, 0, 2, 2)), nil); err != nil {
		t.Fatal(err)
	}
	out, err := groups.ToJPEG(buf.Bytes())
	if err != nil {
		t.Fatalf("ToJPEG: %v", err)
	}
	if !bytes.Equal(out, buf.Bytes()) {
		t.Fatal("a jpeg input must pass through untouched")
	}
}

func TestToJPEGRejectsGarbage(t *testing.T) {
	if _, err := groups.ToJPEG([]byte("not an image")); err == nil {
		t.Fatal("expected an error for a non-image")
	}
}
