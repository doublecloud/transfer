package persqueue

import (
	"sync"
)

type inflight struct {
	batches []int
	items   []*WriteMessage

	lock sync.Mutex
	size int
}

func (s *inflight) Push(data *WriteMessage) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.items = append(s.items, data)
	s.size += len(data.Data)
}

func (s *inflight) Pop() *WriteMessage {
	s.lock.Lock()
	defer s.lock.Unlock()

	if len(s.items) == 0 {
		return nil
	}

	item := s.items[0]
	s.items = s.items[1:len(s.items)]
	s.size -= len(item.Data)
	return item
}

func (s *inflight) IsEmpty() bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	return len(s.items) == 0
}

func (s *inflight) Peek() *WriteMessage {
	s.lock.Lock()
	defer s.lock.Unlock()

	if len(s.items) == 0 {
		return nil
	}

	return s.items[0]
}

func (s *inflight) PeekAll() []*WriteMessage {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.items
}

func (s *inflight) Size() int {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.size
}

func (s *inflight) Length() int {
	s.lock.Lock()
	defer s.lock.Unlock()
	return len(s.items)
}
