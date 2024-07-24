package persqueue

import (
	"fmt"
	"runtime"
	"sync"
)

// Compressor declares expected methods to be implemented by any compressing
// algorithm.
type Compressor interface {
	Compress(data []byte) ([]byte, error)
}

// Decompressor is an interface for a different decompressor providers.
type Decompressor interface {
	Decompress(data []byte) ([]byte, error)
}

type WriteMessageOrError struct {
	Msg *WriteMessage
	Err error
}

type ReadMessageOrError struct {
	Msg ReadMessage
	Err error
}

// TODO(prime@): closed here might cause goroutine leaks. Think of a better way of shutting down.
func Compress(in <-chan *WriteMessage, closed <-chan struct{}, codec Codec, level int, concurrency int) <-chan WriteMessageOrError {
	if concurrency <= 0 {
		concurrency = runtime.NumCPU() - 1
	}
	if codec == Raw {
		return compressRaw(in, closed, concurrency)
	}
	return compressWithPool(in, closed, NewCompressorPool(codec, level), concurrency)
}

// TODO(prime@): closed here might cause goroutine leaks. Think of a better way of shutting down.
func compressRaw(in <-chan *WriteMessage, closed <-chan struct{}, bufferSize int) <-chan WriteMessageOrError {
	out := make(chan WriteMessageOrError, bufferSize)
	go func() {
		defer close(out)
		for {
			select {
			case data, ok := <-in:
				if !ok {
					return
				}

				select {
				case out <- WriteMessageOrError{
					Msg: data,
					Err: nil,
				}:
				case <-closed:
					return
				}

			case <-closed:
				return
			}
		}
	}()
	return out
}

// TODO(prime@): closed here might cause goroutine leaks. Think of a better way of shutting down.
func compressWithPool(in <-chan *WriteMessage, closed <-chan struct{}, compressors CompressorPool, concurrency int) <-chan WriteMessageOrError {
	out := make(chan WriteMessageOrError, concurrency)
	go func() {
		defer close(out)
		zipQ := make(chan chan WriteMessageOrError, concurrency)
		go func() {
			defer close(zipQ)
			for {
				select {
				case data, ok := <-in:
					if !ok {
						return
					}

					zipF := make(chan WriteMessageOrError, 1)
					select {
					case zipQ <- zipF:
						go func(data *WriteMessage) {
							defer close(zipF)
							var msg = WriteMessageOrError{Msg: data}
							var compressor = compressors.Get()
							defer compressors.Put(compressor)

							data.Data, msg.Err = compressor.Compress(data.Data)
							zipF <- msg
						}(data)
					case <-closed:
						return
					}
				case <-closed:
					return
				}
			}
		}()

		for future := range zipQ {
			select {
			case out <- <-future: // пиу пиу из будущего
			case <-closed:
			}
		}
	}()

	return out
}

func Decompress(in <-chan ReadMessage) <-chan ReadMessageOrError {
	out := make(chan ReadMessageOrError)
	go func() {
		defer close(out)
		zipQ := make(chan chan ReadMessageOrError)
		go func() {
			defer close(zipQ)
			for data := range in {
				zipF := make(chan ReadMessageOrError, 1)
				zipQ <- zipF
				go func(msg ReadMessage) {
					// Shortcut for non-compressed chunks.
					if msg.Codec == Raw {
						zipF <- ReadMessageOrError{Msg: msg}
						close(zipF)
						return
					}
					var dec = getDecPool(msg.Codec).Get().(Decompressor)
					var err error
					msg.Data, err = dec.Decompress(msg.Data)
					getDecPool(msg.Codec).Put(dec)
					msg.Codec = Raw
					zipF <- ReadMessageOrError{
						Msg: msg,
						Err: err,
					}
					close(zipF)
				}(data)
			}
		}()
		for future := range zipQ {
			out <- <-future // пиу пиу из будущего
		}
	}()
	return out
}

const (
	unsupportedCodecMgs = "Codec `%s` is not implemented. Please, remove this codec from supported in logbroker topic"
)

// NewCompressorPool creates a compressor pool for a Writer.
func NewCompressorPool(codec Codec, level int) CompressorPool {
	return &simpleCompressorPool{
		pool: sync.Pool{
			New: getCompressorPoolFactoryFunc(codec, level),
		},
	}
}

func getCompressorPoolFactoryFunc(codec Codec, level int) func() interface{} {
	// NOTE Errors on creation of a Compressor do not happen if
	// an empty buffer is passed.
	switch codec {
	case Raw:
		panic("unexpected code branching")
	case Gzip:
		return gzipCompressorFactory(level)
	case Lzop:
		panic(fmt.Sprintf(unsupportedCodecMgs, "Lzop"))
	case Zstd:
		return zstdCompressorFactory(level)
	default:
		panic("unknown codec")
	}
}

func getDecompressorPoolFactoryFunc(codec Codec) func() interface{} {
	// NOTE Errors on creation of a Decompressor do not happen if
	// an empty buffer is passed.
	switch codec {
	case Raw:
		panic("unexpected code branching")
	case Gzip:
		return gzipDecompressorFactory
	case Lzop:
		panic(fmt.Sprintf(unsupportedCodecMgs, "Lzop"))
	case Zstd:
		return zstdDecompressorFactory
	default:
		panic("unknown codec")
	}
}

// Since protocol implies that compressing codec is specified for each message
// separately, we need to have global pools of decompressors.
var (
	poolGzipDecs = &sync.Pool{New: gzipDecompressorFactory}
	poolZstdDecs = &sync.Pool{New: zstdDecompressorFactory}
)

func getDecPool(codec Codec) *sync.Pool {
	switch codec {
	default:
		fallthrough
	case Raw:
		panic("unexpected code branching")
	case Gzip:
		return poolGzipDecs
	case Lzop:
		panic("not implemented")
	case Zstd:
		return poolZstdDecs
	}
}

// CompressorPool keeps and manages compressor instances for a Writer.
// Use NewCompressorPool(codec, level) to create a new pool.
type CompressorPool interface {
	Get() Compressor
	Put(Compressor)
}

// CompressorPoolProvider allows to control compressor pool for a Writer (i.e. share compressors accross several writers).
type CompressorPoolProvider interface {
	Get(codec Codec, compressionLevel int) CompressorPool
}

type simpleCompressorPool struct {
	pool sync.Pool
}

func (c *simpleCompressorPool) Get() Compressor {
	return c.pool.Get().(Compressor)
}

func (c *simpleCompressorPool) Put(compressor Compressor) {
	c.pool.Put(compressor)
}
