// Copyright (c) 2020 Yandex LLC. All rights reserved.
// Author: Andrey Khaliullin <avhaliullin@yandex-team.ru>

package queue

import "github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue/fake/data"

type Reader struct {
	q      *Queue
	signal chan<- struct{}
	offset int
}

func newReader(q *Queue, signal chan<- struct{}, offset int) *Reader {
	return &Reader{
		q:      q,
		signal: signal,
		offset: offset,
	}
}

func (r *Reader) GetReadOffset() int {
	return r.offset
}

func (r *Reader) Read(acc []*data.Message, limit int) []*data.Message {
	prevLen := len(acc)
	acc = r.q.readAvailable(acc, r.offset, limit)
	r.offset += len(acc) - prevLen
	return acc
}

func (r *Reader) Close() {
	r.q.closeReader(r)
}

func (r *Reader) notify() {
	select {
	case r.signal <- struct{}{}:
	default:
	}
}
