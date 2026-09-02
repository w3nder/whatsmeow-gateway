package media

import "fmt"

const (
	FrameAudio    byte = 0x01
	FrameVideo    byte = 0x02
	FrameKeyframe byte = 0x03
	FrameEnd      byte = 0x04
)

func EncodeFrame(kind byte, payload []byte) []byte {
	out := make([]byte, 1+len(payload))
	out[0] = kind
	copy(out[1:], payload)
	return out
}

func DecodeFrame(raw []byte) (kind byte, payload []byte, err error) {
	if len(raw) == 0 {
		return 0, nil, fmt.Errorf("media: decode frame: empty frame")
	}
	return raw[0], raw[1:], nil
}
