//go:build cgo
// +build cgo

package persqueue

import (
	"github.com/DataDog/zstd"
)

const zstdBestCompression = zstd.BestCompression

type zstdCompressor struct {
	compressionLevel int
}
type zstdDecompressor struct {
	buf []byte
}

var _ Compressor = (*zstdCompressor)(nil)
var _ Decompressor = (*zstdDecompressor)(nil)

func (zc *zstdCompressor) Compress(data []byte) ([]byte, error) {
	return zstd.CompressLevel(nil, data, zc.compressionLevel)
}

func (zc *zstdDecompressor) Decompress(data []byte) ([]byte, error) {
	decompressed, err := zstd.Decompress(zc.buf, data)
	if err != nil {
		return nil, err
	}
	// zstd.Decompress tries to put the result into the provided buffer and returns it in case of success.
	// In case the buffer capacity is not enough to fit all the uncompressed data,
	// new slice is allocated and returned
	// Lets reuse this slice for next calls
	zc.buf = decompressed
	res := append([]byte(nil), decompressed...)
	return res, nil
}

func zstdCompressorFactory(level int) func() interface{} {
	d := &zstdCompressor{compressionLevel: level}
	return func() interface{} {
		return d
	}
}

func zstdDecompressorFactory() interface{} {
	return new(zstdDecompressor)
}
