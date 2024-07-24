// Copyright (c) 2020 Yandex LLC. All rights reserved.
// Author: Andrey Khaliullin <avhaliullin@yandex-team.ru>

package topic

import (
	"sync/atomic"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/fake/internal/queue"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/log"
)

// Topic is model for logbroker's topic with partitions
type Topic struct {
	name string

	parts     []*queue.Queue
	writePart uint32
}

func NewTopic(logger log.Logger, name string, parts int) *Topic {
	res := &Topic{
		name:  name,
		parts: make([]*queue.Queue, parts),
	}
	for i := 0; i < parts; i++ {
		res.parts[i] = queue.NewQueue(logger)
	}
	return res
}

func (t *Topic) GetName() string {
	return t.name
}

func (t *Topic) GetQueueLen(part uint32) uint64 {
	return uint64(t.parts[part].Size())
}

func (t *Topic) GetParts() []*queue.Queue {
	res := make([]*queue.Queue, len(t.parts))
	copy(res, t.parts)
	return res
}

func (t *Topic) GetQueue(partition uint32) *queue.Queue {
	return t.parts[partition]
}

func (t *Topic) GetPartCount() int {
	return len(t.parts)
}

// RotatePartForWrite returns partition number for this topic. This method rotates parts with round robin
func (t *Topic) RotatePartForWrite() uint32 {
	writePart := atomic.AddUint32(&t.writePart, 1)
	if writePart >= uint32(len(t.parts)) {
		prev := writePart
		writePart = writePart % uint32(len(t.parts))
		atomic.CompareAndSwapUint32(&t.writePart, prev, writePart)
	}
	return writePart
}
