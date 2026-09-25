package groups_test

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"strings"
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

func encodedPNG(t *testing.T, w, h int) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := png.Encode(&buf, image.NewRGBA(image.Rect(0, 0, w, h))); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func encodedJPEG(t *testing.T, w, h int) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, image.NewRGBA(image.Rect(0, 0, w, h)), nil); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestToJPEGShrinksTheLongerSideToWhatsAppsLimit(t *testing.T) {
	cases := []struct {
		name         string
		in           []byte
		wantW, wantH int
	}{
		{"wide png", encodedPNG(t, 1600, 1200), 640, 480},
		{"wide jpeg", encodedJPEG(t, 2000, 1000), 640, 320},
		{"tall jpeg", encodedJPEG(t, 500, 1500), 213, 640},
		{"small png", encodedPNG(t, 300, 200), 300, 200},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := groups.ToJPEG(tc.in)
			if err != nil {
				t.Fatalf("ToJPEG: %v", err)
			}
			cfg, format, err := image.DecodeConfig(bytes.NewReader(out))
			if err != nil || format != "jpeg" {
				t.Fatalf("output must be jpeg, got %q (%v)", format, err)
			}
			if cfg.Width != tc.wantW || cfg.Height != tc.wantH {
				t.Fatalf("got %dx%d, want %dx%d", cfg.Width, cfg.Height, tc.wantW, tc.wantH)
			}
		})
	}
}

func TestToJPEGRefusesAPhotoTooLargeToDecode(t *testing.T) {
	header := make([]byte, 13)
	binary.BigEndian.PutUint32(header[0:], 20000)
	binary.BigEndian.PutUint32(header[4:], 20000)
	header[8], header[9] = 8, 6
	chunk := append([]byte("IHDR"), header...)
	var buf bytes.Buffer
	buf.WriteString("\x89PNG\r\n\x1a\n")
	_ = binary.Write(&buf, binary.BigEndian, uint32(len(header)))
	buf.Write(chunk)
	_ = binary.Write(&buf, binary.BigEndian, crc32.ChecksumIEEE(chunk))

	if _, err := groups.ToJPEG(buf.Bytes()); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("a 400 megapixel header must be refused before decoding, got %v", err)
	}
}
