package media

import "fmt"

// Frame kinds tag every message on the media websocket, in both directions:
// audio and video carry a decoded/encoded payload, keyframe carries none (it
// is a request, not data), and end has none either (it is the server telling
// the operator the call is over).
const (
	FrameAudio    byte = 0x01
	FrameVideo    byte = 0x02
	FrameKeyframe byte = 0x03
	FrameEnd      byte = 0x04
)

// EncodeFrame prefixes payload with a one-byte kind tag. This is the whole
// wire format: a websocket binary message is exactly one frame, so there is
// no length prefix to get wrong and no framing to lose across message
// boundaries.
func EncodeFrame(kind byte, payload []byte) []byte {
	out := make([]byte, 1+len(payload))
	out[0] = kind
	copy(out[1:], payload)
	return out
}

// DecodeFrame splits a wire message back into its kind and payload. A
// zero-length message carries no kind byte at all, so it is rejected rather
// than silently read as kind 0x00, which is not one of the four kinds above.
func DecodeFrame(raw []byte) (kind byte, payload []byte, err error) {
	if len(raw) == 0 {
		return 0, nil, fmt.Errorf("media: decode frame: empty frame")
	}
	return raw[0], raw[1:], nil
}
