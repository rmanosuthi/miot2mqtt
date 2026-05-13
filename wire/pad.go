package wire

import (
	"fmt"
	"log/slog"
)

const MaxPayloadSize = 1024

// Helper to calculate padded size of a given, probably unpadded src
func getPaddedSize(src []byte, align uint8) int {
	lSrc := len(src)
	blockSize := int(align)
	if lSrc%blockSize == 0 {
		return lSrc + blockSize
	} else {
		return lSrc + (blockSize - (lSrc % blockSize))
	}
}

// PKCS7
func padBytes(src []byte, align uint8) ([]byte, error) {
	// special case: len(src) % align == 0
	// add a whole block
	var need uint8
	lSrc := len(src)
	slog.Debug("payload size", "len", lSrc)
	if lSrc > MaxPayloadSize {
		slog.Warn("payload size exceeded, recv may fail", "len", lSrc)
	}
	blockSize := int(align)
	if lSrc%blockSize == 0 {
		need = align
	} else {
		need = align - uint8(lSrc%blockSize)
	}
	dst := make([]byte, lSrc+int(need))
	copy(dst, src)
	for i := range need {
		dst[lSrc+int(i)] = need
	}
	return dst, nil
}

// PKCS7
func unpadBytes(src []byte, align uint8) ([]byte, error) {
	lSrc := len(src)
	if lSrc == 0 {
		return src, nil
	}

	lastChar := src[lSrc-1]
	padLen := int(lastChar)
	if padLen > lSrc {
		return nil, fmt.Errorf("invalid pad len %v", padLen)
	}
	return src[0 : lSrc-padLen], nil
}
