package groups

import (
	"encoding/binary"
	"image"
)

const (
	orientationNormal = 1
	orientationTag    = 0x0112
	jpegMarkerSOI     = 0xD8
	jpegMarkerSOS     = 0xDA
	jpegMarkerAPP1    = 0xE1
	tiffMagic         = 42
	ifdEntrySize      = 12
)

var exifHeader = []byte("Exif\x00\x00")

func exifOrientation(data []byte) int {
	if len(data) < 4 || data[0] != 0xFF || data[1] != jpegMarkerSOI {
		return orientationNormal
	}
	for pos := 2; pos+4 <= len(data); {
		if data[pos] != 0xFF {
			return orientationNormal
		}
		marker := data[pos+1]
		if marker == jpegMarkerSOS {
			return orientationNormal
		}
		length := int(binary.BigEndian.Uint16(data[pos+2:]))
		end := pos + 2 + length
		if length < 2 || end > len(data) {
			return orientationNormal
		}
		if marker == jpegMarkerAPP1 {
			if value, ok := tiffOrientation(data[pos+4 : end]); ok {
				return value
			}
		}
		pos = end
	}
	return orientationNormal
}

func tiffOrientation(segment []byte) (int, bool) {
	if len(segment) < len(exifHeader)+8 || string(segment[:len(exifHeader)]) != string(exifHeader) {
		return 0, false
	}
	tiff := segment[len(exifHeader):]
	var order binary.ByteOrder
	switch string(tiff[:2]) {
	case "II":
		order = binary.LittleEndian
	case "MM":
		order = binary.BigEndian
	default:
		return 0, false
	}
	if order.Uint16(tiff[2:]) != tiffMagic {
		return 0, false
	}
	offset := uint64(order.Uint32(tiff[4:]))
	if offset+2 > uint64(len(tiff)) {
		return 0, false
	}
	ifd := int(offset)
	entries := int(order.Uint16(tiff[ifd:]))
	for i := range entries {
		entry := ifd + 2 + i*ifdEntrySize
		if entry+ifdEntrySize > len(tiff) {
			return 0, false
		}
		if order.Uint16(tiff[entry:]) == orientationTag {
			value := int(order.Uint16(tiff[entry+8:]))
			if value < 1 || value > 8 {
				return 0, false
			}
			return value, true
		}
	}
	return 0, false
}

func orient(square *image.RGBA, orientation int) *image.RGBA {
	if orientation == orientationNormal {
		return square
	}
	n := square.Bounds().Dx()
	out := image.NewRGBA(square.Bounds())
	last := n - 1
	for y := range n {
		for x := range n {
			var sx, sy int
			switch orientation {
			case 2:
				sx, sy = last-x, y
			case 3:
				sx, sy = last-x, last-y
			case 4:
				sx, sy = x, last-y
			case 5:
				sx, sy = y, x
			case 6:
				sx, sy = y, last-x
			case 7:
				sx, sy = last-y, last-x
			case 8:
				sx, sy = last-y, x
			default:
				sx, sy = x, y
			}
			out.SetRGBA(x, y, square.RGBAAt(sx, sy))
		}
	}
	return out
}
