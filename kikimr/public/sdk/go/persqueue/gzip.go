package persqueue

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"runtime"
	"sync"
)

type gzipCompressor struct {
	*gzip.Writer
}

var _ Compressor = (*gzipCompressor)(nil)

func (gc *gzipCompressor) Compress(data []byte) ([]byte, error) {
	buf := gzipBuffersPool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		gzipBuffersPool.Put(buf)
	}()

	gc.Reset(buf)

	if _, err := gc.Write(data); err != nil {
		return nil, err
	}

	if err := gc.Flush(); err != nil {
		return nil, err
	}

	if err := gc.Close(); err != nil {
		return nil, err
	}

	return append([]byte(nil), buf.Bytes()...), nil
}

func gzipCompressorFactory(level int) func() interface{} {
	return func() interface{} {
		enc, _ := gzip.NewWriterLevel(nil, level)
		runtime.SetFinalizer(enc, func(e *gzip.Writer) { _ = e.Close() })
		return &gzipCompressor{Writer: enc}
	}
}

type gzipDecompressor struct {
	*gzip.Reader
}

var _ Decompressor = (*gzipDecompressor)(nil)

func (gd *gzipDecompressor) Decompress(data []byte) ([]byte, error) {
	reader := gzipReadersPool.Get().(*bytes.Reader)
	defer func() {
		reader.Reset(nil)
		gzipReadersPool.Put(reader)
	}()

	reader.Reset(data)
	if err := gd.Reset(reader); err != nil {
		return nil, err
	}
	defer func() {
		_ = gd.Close()
	}()
	return ioutil.ReadAll(gd)
}

func gzipDecompressorFactory() interface{} {
	dec := new(gzip.Reader)
	runtime.SetFinalizer(dec, func(d *gzip.Reader) { _ = d.Close() })
	return &gzipDecompressor{Reader: dec}
}

var (
	gzipBuffersPool = sync.Pool{New: func() interface{} { return &bytes.Buffer{} }}
	gzipReadersPool = sync.Pool{New: func() interface{} { return bytes.NewReader(nil) }}
)
