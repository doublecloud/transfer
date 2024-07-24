package persqueue

import (
	"compress/flate"
	"compress/gzip"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestWrapLeak(t *testing.T) {
	cases := []struct {
		Name  string
		Codec Codec
		Level int
	}{
		{
			Name:  "gzip",
			Codec: Gzip,
			Level: flate.BestCompression,
		},
		{
			Name:  "zstd",
			Codec: Zstd,
			Level: flate.BestCompression,
		},
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			defer goleak.VerifyNone(t)
			in := make(chan *WriteMessage)
			wrap := Compress(in, nil, tc.Codec, tc.Level, 0)
			in <- &WriteMessage{Data: []byte("hello")}
			gzipped := <-wrap

			require.NoError(t, gzipped.Err)

			close(in)
			_, ok := <-wrap
			require.False(t, ok)
		})
	}
}

func BenchmarkCompressor(b *testing.B) {
	cases := []struct {
		Name  string
		Codec Codec
		Level int
	}{
		{
			Name:  "gzip",
			Codec: Gzip,
			Level: flate.BestCompression,
		},
		{
			Name:  "zstd",
			Codec: Zstd,
			Level: zstdBestCompression, // best compression
		},
	}
	for _, tc := range cases {
		b.Run(tc.Name, func(b *testing.B) {
			in := make(chan *WriteMessage)
			wrap := Compress(in, nil, tc.Codec, tc.Level, 0)

			for n := 0; n < b.N; n++ {
				in <- &WriteMessage{Data: []byte("hello")}
				gzipped := <-wrap

				require.NoError(b, gzipped.Err)
			}
			close(in)
			_, ok := <-wrap
			require.False(b, ok)
			b.SetBytes(int64(5 * b.N))
			b.ReportAllocs()
		})
	}
}

func BenchmarkDecompressor(b *testing.B) {
	gzipC, err := gzip.NewWriterLevel(nil, flate.BestCompression)
	require.NoError(b, err)
	cases := []struct {
		Name       string
		Compressor Compressor
		Codec      Codec
	}{
		{
			Name:       "gzip",
			Compressor: &gzipCompressor{Writer: gzipC},
			Codec:      Gzip,
		},
		{
			Name:       "zstd",
			Compressor: zstdCompressorFactory(zstdBestCompression)().(*zstdCompressor),
			Codec:      Zstd,
		},
	}
	for _, tc := range cases {
		b.Run(tc.Name, func(b *testing.B) {
			compressed, _ := tc.Compressor.Compress([]byte("hello"))
			compressedIn := make(chan ReadMessage)
			defer close(compressedIn)
			unwrap := Decompress(compressedIn)
			for n := 0; n < b.N; n++ {
				compressedIn <- ReadMessage{
					Data:  compressed,
					Codec: tc.Codec,
				}
				decompressed := <-unwrap
				require.NoError(b, decompressed.Err)
				require.Equal(b, string(decompressed.Msg.Data), "hello")
			}
			b.SetBytes(int64(5 * b.N))
			b.ReportAllocs()
		})
	}
}

func TestUnwrapLeak(t *testing.T) {
	gzipC, err := gzip.NewWriterLevel(nil, flate.BestCompression)
	require.NoError(t, err)
	cases := []struct {
		Name       string
		Compressor Compressor
		Codec      Codec
	}{
		{
			Name:       "gzip",
			Compressor: &gzipCompressor{Writer: gzipC},
			Codec:      Gzip,
		},
		{
			Name:       "zstd",
			Compressor: zstdCompressorFactory(zstdBestCompression)().(*zstdCompressor),
			Codec:      Zstd,
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			if tc.Name != "zstd" {
				defer goleak.VerifyNone(t)
			}
			compressed, _ := tc.Compressor.Compress([]byte("hello"))

			compressedIn := make(chan ReadMessage)
			defer close(compressedIn)
			unwrap := Decompress(compressedIn)
			compressedIn <- ReadMessage{
				Data:  compressed,
				Codec: tc.Codec,
			}
			decompressed := <-unwrap

			require.NoError(t, decompressed.Err)
			require.Equal(t, string(decompressed.Msg.Data), "hello")

			compressed2, _ := tc.Compressor.Compress([]byte("hello2"))
			compressedIn <- ReadMessage{
				Data:  compressed2,
				Codec: tc.Codec,
			}
			decompressed = <-unwrap

			require.NoError(t, decompressed.Err)
			require.Equal(t, string(decompressed.Msg.Data), "hello2")
		})
	}
}
