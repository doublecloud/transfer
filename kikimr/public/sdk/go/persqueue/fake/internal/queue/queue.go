// Copyright (c) 2019 Yandex LLC. All rights reserved.
// Author: Andrey Khaliullin <avhaliullin@yandex-team.ru>

package queue

import (
	"context"
	"sync"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue/fake/data"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue/log"
)

// Queue is fake logbroker topic message queue implementation. Queue stores all written messages, and never gets cleaned up
type Queue struct {
	logger log.Logger

	messages []*data.Message
	mu       sync.RWMutex
	// readCond used as sync.Cond with read chan == Cond.Wait, close(chan) == Cond.Broadcast().
	// Chan used instead of Cond in order to listen to Context during wait
	readCond chan struct{}

	readers   map[*Reader]struct{}
	readersMu sync.Mutex
}

func NewQueue(logger log.Logger) *Queue {
	return &Queue{
		logger:   logger,
		readCond: make(chan struct{}),
		readers:  make(map[*Reader]struct{}),
	}
}

func (q *Queue) Size() int {
	q.mu.RLock()
	res := len(q.messages)
	q.mu.RUnlock()
	return res
}

func (q *Queue) Write(msgs []*data.Message) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	l := len(q.messages)
	for i, msg := range msgs {
		msg.Offset = l + i
	}
	q.messages = append(q.messages, msgs...)
	close(q.readCond)
	q.initReadCond()
	q.notifyReaders()
	return l
}

func (q *Queue) notifyReaders() {
	q.readersMu.Lock()
	for r := range q.readers {
		r.notify()
	}
	q.readersMu.Unlock()
}

func (q *Queue) CreateReader(signal chan<- struct{}, offset int) *Reader {
	r := newReader(q, signal, offset)
	q.readersMu.Lock()
	q.readers[r] = struct{}{}
	q.readersMu.Unlock()
	r.notify()
	return r
}

func (q *Queue) ReadAsync(ctx context.Context, offset int, out chan<- *data.Message) {
	go q.read(ctx, offset, out)
}

func (q *Queue) readAvailable(acc []*data.Message, offset int, limit int) []*data.Message {
	limit -= len(acc)
	if limit <= 0 {
		return acc
	}
	q.mu.RLock()
	idx := 0
	for ; idx < limit && (offset+idx) < len(q.messages); idx++ {
		acc = append(acc, q.messages[offset+idx])
	}
	q.mu.RUnlock()
	return acc
}

func (q *Queue) read(ctx context.Context, offset int, out chan<- *data.Message) {
	for ctx.Err() == nil {
		q.mu.RLock()
		var tail []*data.Message
		var cond chan struct{}
		if len(q.messages) > offset {
			tail = q.messages[offset:]
			offset += len(tail)
		}
		cond = q.readCond
		q.mu.RUnlock()
		for _, msg := range tail {
			out <- msg
		}
		select {
		case <-ctx.Done():
			return
		case <-cond: // continue
		}
	}
}

func (q *Queue) initReadCond() {
	q.readCond = make(chan struct{})
}

func (q *Queue) closeReader(r *Reader) {
	q.readersMu.Lock()
	delete(q.readers, r)
	q.readersMu.Unlock()
}
