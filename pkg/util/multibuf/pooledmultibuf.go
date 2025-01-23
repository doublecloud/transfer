package multibuf

import (
	"bytes"
	"io"
	"sync"
)

// PooledMultiBuffer provides io.Reader interface over multiple bytes.Buffers (like io.MultiReader)
// buffers are taken from sync.Pool to reduce GC pressure.
type PooledMultiBuffer struct {
	buffers    []io.Reader
	bufMu      sync.Mutex
	rd         io.Reader
	onceReader sync.Once
	pool       *sync.Pool
}

func (q *PooledMultiBuffer) initReaderOnce() {
	q.onceReader.Do(func() { q.rd = io.MultiReader(q.buffers...) })
}

// Read implements io.Reader interface
// It is guaranteed that internal buffers read order is the same as the order of AcquireBuffer calls.
func (q *PooledMultiBuffer) Read(p []byte) (int, error) {
	q.initReaderOnce()
	return q.rd.Read(p)
}

// AcquireBuffer provides a buffer. It may allocate new buffer in memory or take it from internal memory pool.
// To optimize memory allocations you can set initial buffer capacity if it is known.
// It is guaranteed that internal buffers read order is the same as the order of AcquireBuffer calls.
func (q *PooledMultiBuffer) AcquireBuffer(initialCap int) *bytes.Buffer {
	var buf *bytes.Buffer
	if b := q.pool.Get(); b != nil {
		buf = b.(*bytes.Buffer)
		buf.Reset()
	} else {
		buf = bytes.NewBuffer(make([]byte, 0, initialCap))
	}
	q.bufMu.Lock()
	defer q.bufMu.Unlock()
	q.buffers = append(q.buffers, buf)
	return buf
}

// Close release all acquired buffers and return them into memory pool
// Read should not be called after close.
func (q *PooledMultiBuffer) Close() {
	for _, buf := range q.buffers {
		q.pool.Put(buf)
	}
	q.buffers = nil
	q.rd = nil
}

// NewPooledMultiBuffer makes new PooledMiltiBuffer.
// Pool is used to manage memory for buffers and reuse them between multiple PooledMultiBuffers
// Count is used as a hint to possible buffer count. You may provide 0 as initial count.
func NewPooledMultiBuffer(count int, pool *sync.Pool) *PooledMultiBuffer {
	return &PooledMultiBuffer{
		buffers:    make([]io.Reader, 0, count),
		bufMu:      sync.Mutex{},
		rd:         nil,
		onceReader: sync.Once{},
		pool:       pool,
	}
}
