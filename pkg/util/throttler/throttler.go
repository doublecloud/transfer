package throttler

import "sync"

type MemoryThrottler struct {
	BufferSize    uint64 // 0 means turned-off
	inflightMutex sync.Mutex
	inflightBytes uint64
}

func (t *MemoryThrottler) ExceededLimits() bool {
	t.inflightMutex.Lock()
	defer t.inflightMutex.Unlock()
	return t.inflightBytes > t.BufferSize && t.BufferSize != 0
}

func (t *MemoryThrottler) AddInflight(size uint64) {
	t.inflightMutex.Lock()
	defer t.inflightMutex.Unlock()
	t.inflightBytes += size
}

func (t *MemoryThrottler) ReduceInflight(size uint64) {
	t.inflightMutex.Lock()
	defer t.inflightMutex.Unlock()
	t.inflightBytes -= size
}

func NewMemoryThrottler(bufferSize uint64) *MemoryThrottler {
	return &MemoryThrottler{
		BufferSize:    bufferSize,
		inflightMutex: sync.Mutex{},
		inflightBytes: 0,
	}
}
