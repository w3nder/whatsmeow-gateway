package groups

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"image/color"
	_ "image/gif"
	"image/jpeg"
	_ "image/png"

	"golang.org/x/image/draw"
)

const (
	jpegQuality     = 85
	maxPhotoSide    = 640
	maxPhotoPixels  = 16_000_000
	photoSlotCount  = 2
	jpegImageFormat = "jpeg"
)

var photoSlots = make(chan struct{}, photoSlotCount)

func ToJPEG(ctx context.Context, data []byte) ([]byte, error) {
	select {
	case photoSlots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-photoSlots }()

	cfg, format, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("groups: decode photo: %w", err)
	}
	if cfg.Width*cfg.Height > maxPhotoPixels {
		return nil, fmt.Errorf("groups: photo of %dx%d exceeds %d pixels", cfg.Width, cfg.Height, maxPhotoPixels)
	}
	orientation := orientationNormal
	if format == jpegImageFormat {
		orientation = exifOrientation(data)
	}
	if format == jpegImageFormat && orientation == orientationNormal && cfg.Width == cfg.Height && cfg.Width <= maxPhotoSide {
		return data, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	img, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("groups: decode photo: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	if err := jpeg.Encode(&out, orient(squareOnWhite(img, maxPhotoSide), orientation), &jpeg.Options{Quality: jpegQuality}); err != nil {
		return nil, fmt.Errorf("groups: encode photo as jpeg: %w", err)
	}
	return out.Bytes(), nil
}

func squareOnWhite(img image.Image, maxSide int) *image.RGBA {
	bounds := img.Bounds()
	side := min(bounds.Dx(), bounds.Dy())
	crop := image.Rect(0, 0, side, side).Add(bounds.Min).Add(image.Pt((bounds.Dx()-side)/2, (bounds.Dy()-side)/2))
	target := min(side, maxSide)
	square := image.NewRGBA(image.Rect(0, 0, target, target))
	draw.Draw(square, square.Bounds(), image.NewUniform(color.White), image.Point{}, draw.Src)
	draw.CatmullRom.Scale(square, square.Bounds(), img, crop, draw.Over, nil)
	return square
}
