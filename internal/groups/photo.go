package groups

import (
	"bytes"
	"fmt"
	"image"
	_ "image/gif"
	"image/jpeg"
	_ "image/png"
)

const jpegQuality = 85

func ToJPEG(data []byte) ([]byte, error) {
	_, format, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("groups: decode photo: %w", err)
	}
	if format == "jpeg" {
		return data, nil
	}
	img, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("groups: decode photo: %w", err)
	}
	var out bytes.Buffer
	if err := jpeg.Encode(&out, img, &jpeg.Options{Quality: jpegQuality}); err != nil {
		return nil, fmt.Errorf("groups: encode photo as jpeg: %w", err)
	}
	return out.Bytes(), nil
}
