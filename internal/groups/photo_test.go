package groups_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
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

	out, err := groups.ToJPEG(context.Background(), buf.Bytes())
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
	out, err := groups.ToJPEG(context.Background(), buf.Bytes())
	if err != nil {
		t.Fatalf("ToJPEG: %v", err)
	}
	if !bytes.Equal(out, buf.Bytes()) {
		t.Fatal("a jpeg input must pass through untouched")
	}
}

func TestToJPEGRejectsGarbage(t *testing.T) {
	if _, err := groups.ToJPEG(context.Background(), []byte("not an image")); err == nil {
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

func TestToJPEGCropsASquareAndShrinksItToWhatsAppsLimit(t *testing.T) {
	cases := []struct {
		name         string
		in           []byte
		wantW, wantH int
	}{
		{"wide png", encodedPNG(t, 1600, 1200), 640, 640},
		{"wide jpeg", encodedJPEG(t, 2000, 1000), 640, 640},
		{"tall jpeg", encodedJPEG(t, 500, 1500), 500, 500},
		{"small png", encodedPNG(t, 300, 200), 200, 200},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := groups.ToJPEG(context.Background(), tc.in)
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

func pngHeader(w, h uint32) []byte {
	header := make([]byte, 13)
	binary.BigEndian.PutUint32(header[0:], w)
	binary.BigEndian.PutUint32(header[4:], h)
	header[8], header[9] = 8, 6
	chunk := append([]byte("IHDR"), header...)
	var buf bytes.Buffer
	buf.WriteString("\x89PNG\r\n\x1a\n")
	_ = binary.Write(&buf, binary.BigEndian, uint32(len(header)))
	buf.Write(chunk)
	_ = binary.Write(&buf, binary.BigEndian, crc32.ChecksumIEEE(chunk))
	return buf.Bytes()
}

func TestToJPEGRefusesAPhotoTooLargeToDecode(t *testing.T) {
	if _, err := groups.ToJPEG(context.Background(), pngHeader(5000, 4000)); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("a 20 megapixel header must be refused before decoding, got %v", err)
	}
}

func TestToJPEGStopsWhenTheContextIsDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := groups.ToJPEG(ctx, encodedPNG(t, 800, 800)); !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled context must stop the conversion, got %v", err)
	}
}

func TestToJPEGPaintsTransparencyWhite(t *testing.T) {
	var buf bytes.Buffer
	if err := png.Encode(&buf, image.NewNRGBA(image.Rect(0, 0, 64, 64))); err != nil {
		t.Fatal(err)
	}
	out, err := groups.ToJPEG(context.Background(), buf.Bytes())
	if err != nil {
		t.Fatalf("ToJPEG: %v", err)
	}
	img, err := jpeg.Decode(bytes.NewReader(out))
	if err != nil {
		t.Fatal(err)
	}
	if r, g, b, _ := img.At(32, 32).RGBA(); r>>8 < 240 || g>>8 < 240 || b>>8 < 240 {
		t.Fatalf("a transparent pixel must become white, got %d %d %d", r>>8, g>>8, b>>8)
	}
}

func withOrientation(t *testing.T, jpegData []byte, orientation uint16) []byte {
	t.Helper()
	ifd := make([]byte, 2+12+4)
	binary.LittleEndian.PutUint16(ifd[0:], 1)
	binary.LittleEndian.PutUint16(ifd[2:], 0x0112)
	binary.LittleEndian.PutUint16(ifd[4:], 3)
	binary.LittleEndian.PutUint32(ifd[6:], 1)
	binary.LittleEndian.PutUint16(ifd[10:], orientation)
	tiff := append([]byte{'I', 'I', 42, 0, 8, 0, 0, 0}, ifd...)
	payload := append([]byte("Exif\x00\x00"), tiff...)
	segment := []byte{0xFF, 0xE1, 0, 0}
	binary.BigEndian.PutUint16(segment[2:], uint16(len(payload)+2))
	segment = append(segment, payload...)
	out := append([]byte{}, jpegData[:2]...)
	out = append(out, segment...)
	return append(out, jpegData[2:]...)
}

func TestToJPEGAppliesTheExifOrientation(t *testing.T) {
	src := image.NewRGBA(image.Rect(0, 0, 64, 64))
	for y := range 64 {
		for x := range 64 {
			c := color.RGBA{B: 255, A: 255}
			if x < 32 {
				c = color.RGBA{R: 255, A: 255}
			}
			src.Set(x, y, c)
		}
	}
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, src, &jpeg.Options{Quality: 95}); err != nil {
		t.Fatal(err)
	}

	out, err := groups.ToJPEG(context.Background(), withOrientation(t, buf.Bytes(), 6))
	if err != nil {
		t.Fatalf("ToJPEG: %v", err)
	}
	img, err := jpeg.Decode(bytes.NewReader(out))
	if err != nil {
		t.Fatal(err)
	}
	top, _, _, _ := img.At(32, 8).RGBA()
	bottom, _, _, _ := img.At(32, 56).RGBA()
	if top>>8 < 200 || bottom>>8 > 60 {
		t.Fatalf("orientation 6 turns the red left half to the top, got red %d on top and %d at the bottom", top>>8, bottom>>8)
	}
}
