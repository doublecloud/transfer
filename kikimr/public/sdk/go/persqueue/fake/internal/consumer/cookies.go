// Copyright (c) 2019 Yandex LLC. All rights reserved.
// Author: Andrey Khaliullin <avhaliullin@yandex-team.ru>

package consumer

import (
	"container/list"
	"sync"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue/fake/data"
)

type cookieID = int

type cookieStore struct {
	counter       int
	cookiesByID   map[cookieID]*cookie
	cookiesByPart map[partKey]*list.List
	mu            sync.Mutex
}

func newCookieStore() *cookieStore {
	return &cookieStore{
		cookiesByID:   make(map[cookieID]*cookie),
		cookiesByPart: make(map[partKey]*list.List),
	}
}

func (s *cookieStore) BindCookie(msgs []*data.Message) cookieID {
	s.mu.Lock()
	defer s.mu.Unlock()
	cookieID := s.counter
	s.counter++
	cookieOffsets := make(partOffsets)
	for _, msg := range msgs {
		key := partKey{topic: msg.Topic, part: msg.Partition}
		cookieOffsets[key] = uint64(msg.Offset + 1)
	}
	c := &cookie{partToOffset: cookieOffsets}
	s.cookiesByID[cookieID] = c
	for key := range cookieOffsets {
		partCookies, ok := s.cookiesByPart[key]
		if !ok {
			partCookies = list.New()
			s.cookiesByPart[key] = partCookies
		}
		partCookies.PushBack(c)
	}
	return cookieID
}

func (s *cookieStore) Commit(ids []cookieID) partOffsets {
	s.mu.Lock()
	defer s.mu.Unlock()
	partsToTest := make(map[partKey]struct{})
	// in first pass we set commit attribute and collect all parts that can be affected
	for _, id := range ids {
		c, ok := s.cookiesByID[id]
		if !ok {
			continue
		}
		c.committed = true

		for key := range c.partToOffset {
			partsToTest[key] = struct{}{}
		}
	}
	res := make(map[partKey]uint64)
	// in seconds pass we iterating cookies for partitions and collecting offsets until uncommitted cookie met
	for key := range partsToTest {
		partList := s.cookiesByPart[key]
		e := partList.Front()
		for e != nil {
			c := e.Value.(*cookie)
			if !c.committed {
				break
			}
			e = e.Next()
			res[key] = c.partToOffset[key]
			delete(c.partToOffset, key)
		}
	}
	return res
}

type cookie struct {
	partToOffset partOffsets
	committed    bool
}

type partOffsets = map[partKey]uint64
type partKey struct {
	topic string
	part  uint32
}
