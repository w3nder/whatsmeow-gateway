package groups

import (
	"bytes"
	"fmt"
	"image"
	_ "image/gif"
	"image/jpeg"
	_ "image/png"

	"golang.org/x/image/draw"
)

const (
	jpegQuality     = 85
	maxPhotoSide    = 640
	maxPhotoPixels  = 50_000_000
	jpegImageFormat = "jpeg"
)

func ToJPEG(data []byte) ([]byte, error) {
	cfg, format, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("groups: decode photo: %w", err)
	}
	if cfg.Width*cfg.Height > maxPhotoPixels {
		return nil, fmt.Errorf("groups: photo of %dx%d exceeds %d pixels", cfg.Width, cfg.Height, maxPhotoPixels)
	}
	if format == jpegImageFormat && max(cfg.Width, cfg.Height) <= maxPhotoSide {
		return data, nil
	}
	img, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("groups: decode photo: %w", err)
	}
	var out bytes.Buffer
	if err := jpeg.Encode(&out, fitWithin(img, maxPhotoSide), &jpeg.Options{Quality: jpegQuality}); err != nil {
		return nil, fmt.Errorf("groups: encode photo as jpeg: %w", err)
	}
	return out.Bytes(), nil
}

func fitWithin(img image.Image, side int) image.Image {
	bounds := img.Bounds()
	w, h := bounds.Dx(), bounds.Dy()
	if max(w, h) <= side {
		return img
	}
	if w >= h {
		w, h = side, max(1, h*side/w)
	} else {
		w, h = max(1, w*side/h), side
	}
	scaled := image.NewRGBA(image.Rect(0, 0, w, h))
	draw.CatmullRom.Scale(scaled, scaled.Bounds(), img, bounds, draw.Src, nil)
	return scaled
}
