package persqueue

import (
	"sync"
)

type commitBuffer struct {
	size    int
	maxSize int

	enqueuedCommits []PartitionCookie
	inflightCommits map[PartitionCookie]struct{}
	cookies         map[PartitionCookie]struct{}

	m sync.Mutex
}

func newCommitBuffer(maxSize int) *commitBuffer {
	return &commitBuffer{
		maxSize:         maxSize,
		inflightCommits: make(map[PartitionCookie]struct{}),
		cookies:         make(map[PartitionCookie]struct{}),
	}
}

func (b *commitBuffer) InflightMessages() int {
	b.m.Lock()
	defer b.m.Unlock()
	return len(b.cookies)
}

func (b *commitBuffer) Size() int {
	b.m.Lock()
	defer b.m.Unlock()
	return b.size
}

func (b *commitBuffer) InLimits() bool {
	b.m.Lock()
	defer b.m.Unlock()
	return b.maxSize == 0 || b.size <= b.maxSize
}

func (b *commitBuffer) AddCookie(cookie PartitionCookie, size int) {
	b.m.Lock()
	defer b.m.Unlock()

	b.cookies[cookie] = struct{}{}
	b.size += size
}

func (b *commitBuffer) EnqueueCommit(cookie PartitionCookie, size int) {
	b.m.Lock()
	defer b.m.Unlock()

	delete(b.cookies, cookie)
	b.enqueuedCommits = append(b.enqueuedCommits, cookie)
	b.inflightCommits[cookie] = struct{}{}
	b.size -= size
}

func (b *commitBuffer) AckCommit(cookie PartitionCookie) {
	b.m.Lock()
	defer b.m.Unlock()

	delete(b.inflightCommits, cookie)
}

func (b *commitBuffer) PopEnqueuedCommits() []PartitionCookie {
	b.m.Lock()
	defer b.m.Unlock()

	commits := b.enqueuedCommits
	b.enqueuedCommits = nil
	return commits
}

func (b *commitBuffer) AllCommitsAcked() bool {
	b.m.Lock()
	defer b.m.Unlock()

	return len(b.enqueuedCommits) == 0 && len(b.inflightCommits) == 0
}

func (b *commitBuffer) NumInflightCommits() int {
	b.m.Lock()
	defer b.m.Unlock()

	return len(b.inflightCommits)
}
