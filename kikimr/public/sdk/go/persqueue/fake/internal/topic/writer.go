// Copyright (c) 2019 Yandex LLC. All rights reserved.
// Author: Andrey Khaliullin <avhaliullin@yandex-team.ru>

package topic

import (
	"context"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue/fake/data"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue/fake/internal/queue"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue/log"
)

const (
	writeReqsCount = 64
)

// Writer is structure holding write state of some lb's topic for some source. It holds last written seqNo
// and acts like logbroker on write. Write is bind to some specific partition of topic (because source is always
// located in single partition in lockbroker model)
type Writer struct {
	ctx    context.Context
	logger log.Logger

	q       *queue.Queue
	part    uint32
	topic   string
	source  string
	seqNo   uint64
	reqChan chan *WriteReq
}

func NewWriter(ctx context.Context, logger log.Logger, part uint32, q *queue.Queue, topic string, source string) *Writer {
	res := &Writer{
		logger:  logger,
		part:    part,
		topic:   topic,
		source:  source,
		ctx:     ctx,
		q:       q,
		reqChan: make(chan *WriteReq, writeReqsCount),
	}
	go res.eventLoop()
	return res
}

func (w *Writer) RequestsChan() chan<- *WriteReq {
	return w.reqChan
}

func (w *Writer) MaxSeqNo() uint64 {
	return w.seqNo
}

func (w *Writer) eventLoop() {
	for w.ctx.Err() == nil {
		select {
		case <-w.ctx.Done():
			return
		case req := <-w.reqChan:
			w.handleWriteReq(req)
		}
	}
}

func (w *Writer) handleWriteReq(req *WriteReq) {
	var batch []*data.Message
	acks := make([]MsgAck, len(req.Messages))
	for i, msg := range req.Messages {
		acks[i].SeqNo = msg.SeqNo
		if msg.SeqNo <= w.seqNo {
			acks[i].AlreadyWritten = true
			continue
		}
		batch = append(batch, &data.Message{
			Topic:        w.topic,
			Source:       w.source,
			Partition:    w.part,
			WriteMessage: *msg,
		})
	}
	offset := w.q.Write(batch)
	for i := range acks {
		if !acks[i].AlreadyWritten {
			acks[i].Offset = offset
			offset++
		}
	}
	w.writeResponse(req.ResponseCh, &ResponseAck{Acks: acks})
}

func (w *Writer) writeResponse(ch chan<- *ResponseAck, resp *ResponseAck) {
	select {
	case <-w.ctx.Done():
	case ch <- resp:
	}
}

type WriteReq struct {
	Messages   []*data.WriteMessage
	ResponseCh chan<- *ResponseAck
}

type MsgAck struct {
	SeqNo          uint64
	Offset         int
	AlreadyWritten bool
}
type ResponseAck struct {
	Acks []MsgAck
}
