// Copyright (c) 2019 Yandex LLC. All rights reserved.
// Author: Andrey Khaliullin <avhaliullin@yandex-team.ru>

package consumer

import (
	"context"
	"fmt"
	"sync"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue/fake/data"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue/fake/internal/broker"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue/fake/internal/model"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue/fake/internal/topic"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue/log"
)

const (
	maxReadReqs = 128
	reqsQueue   = 10
)

// Session is model for logbroker's read session state - with cookies etc. This is actually fully-featured LB consumer
// but in terms of model objects, not grpc
type Session struct {
	ctx    context.Context
	cancel context.CancelFunc
	props  *ReadSessionProps

	b          *broker.Broker
	logger     log.Logger
	reqs       chan Request
	readReqs   chan *ReadReq
	responses  chan<- Response
	cookies    *cookieStore
	readers    map[string]topic.ReadSession
	readSignal chan struct{}

	close sync.Once
}

func NewSession(ctx context.Context, logger log.Logger, b *broker.Broker, props *ReadSessionProps,
	responses chan<- Response) (*Session, error) {
	ctx, cancel := context.WithCancel(ctx)
	res := &Session{
		ctx:        ctx,
		cancel:     cancel,
		props:      props,
		b:          b,
		logger:     logger,
		reqs:       make(chan Request, reqsQueue),
		readReqs:   make(chan *ReadReq, maxReadReqs),
		responses:  responses,
		cookies:    newCookieStore(),
		readers:    make(map[string]topic.ReadSession),
		readSignal: make(chan struct{}, 1),
	}
	err := res.init()
	if err != nil {
		res.onClose()
		return nil, err
	}
	return res, nil
}

func (s *Session) init() error {
	for _, t := range s.props.Topics {
		r, err := s.b.GetTopicReader(t, s.props.ClientID)
		if err != nil {

			return err
		}
		s.readers[t] = r.CreateSession((*readDispatcher)(s), s.props.WaitCommitOnRelease)
	}

	for _, r := range s.readers {
		r.Start()
	}
	go s.readLoop()
	go s.messagesLoop()
	return nil
}

func (s *Session) Requests() chan<- Request {
	return s.reqs
}

func (s *Session) ClosedCh() <-chan struct{} {
	return s.ctx.Done()
}

func (s *Session) onClose() {
	s.close.Do(func() {
		s.cancel()
		for _, r := range s.readers {
			r.Close()
		}
		s.logger.Log(s.ctx, log.LevelInfo, "closing consumer session", nil)
	})
}

func (s *Session) readLoop() {
	defer s.onClose()
	for s.ctx.Err() == nil {
		select {
		case req := <-s.reqs:
			err := s.processReq(req)
			if err != nil {
				s.logger.Log(s.ctx, log.LevelError, "error processing request", map[string]interface{}{
					"error": err.Error(),
				})
				return
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Session) processReq(req Request) error {
	switch modelReq := req.(type) {
	case *ReadReq:
		return s.processReadReq(modelReq)
	case *CommitReq:
		return s.processCommitReq(modelReq)
	case *StartReadReq:
		return s.processStartReadReq(modelReq)
	default:
		return fmt.Errorf("unknown request %v", req)
	}
}

func (s *Session) processReadReq(req *ReadReq) error {
	select {
	case s.readReqs <- req:
		return nil
	default:
		return fmt.Errorf("too many inflight read requests")
	}
}

func (s *Session) processCommitReq(req *CommitReq) error {
	offsets := s.cookies.Commit(req.Cookies)
	for key, offset := range offsets {
		r := s.readers[key.topic]
		r.Commit(key.part, offset)
	}
	s.writeResponse(&CommitResp{Cookies: req.Cookies})
	return nil
}

func (s *Session) processStartReadReq(req *StartReadReq) error {
	r, ok := s.readers[req.Topic]
	if !ok {
		return fmt.Errorf("unknown topic in StartReadRequest: %s", req.Topic)
	}
	return r.ConfirmLock(req.Generation, req.ReadOffset, req.CommitOffset, req.VerifyOffset)
}

func (s *Session) messagesLoop() {
	defer s.onClose()
	readReqsQueue := s.readReqs
	var readSignal <-chan struct{}
	var readReq *ReadReq

	for s.ctx.Err() == nil {
		select {
		case <-s.ctx.Done():
		case <-readSignal:
			if s.sendMessages(readReq) {
				// if messages were sent - we going to "wait read req mode"
				readSignal = nil
				readReqsQueue = s.readReqs
			}
		case readReq = <-readReqsQueue:
			if !s.sendMessages(readReq) {
				// if we got read request but no new messages - we going to "wait read signal mode"
				readSignal = s.readSignal
				readReqsQueue = nil
			}
		}
	}
}

func (s *Session) sendMessages(req *ReadReq) bool {
	limit := req.MaxMessages
	var messages []*data.Message
	for _, r := range s.readers {
		messages = r.ReadTo(messages, limit)
		if len(messages) == limit {
			break
		}
	}
	if len(messages) == 0 {
		return false
	}
	cookie := s.cookies.BindCookie(messages)
	s.writeResponse(&MessagesResp{
		Cookie:   cookie,
		Messages: messages,
	})
	return true
}

func (s *Session) lockPartition(genID uint64, p *model.PartitionInfo) {
	s.logger.Log(s.ctx, log.LevelDebug, "lock partition", map[string]interface{}{
		"gen":  genID,
		"info": p,
	})
	if s.props.ExplicitPartitions {
		s.writeResponse(&LockResp{
			Generation: genID,
			Partition:  p,
		})
	} else {
		err := s.readers[p.Topic].ConfirmLock(genID, p.ReadOffset, 0, false)
		if err != nil {
			s.logger.Log(s.ctx, log.LevelError, "fatal error: instant lock confirmation failed", map[string]interface{}{
				"error": err.Error(),
			})
			s.cancel()
		}
	}
}

func (s *Session) releasePartition(genID uint64, topicName string, partition uint32, mustCommit bool) {
	s.logger.Log(s.ctx, log.LevelDebug, "release partition", map[string]interface{}{
		"gen":       genID,
		"topic":     topicName,
		"partition": partition,
	})
	if s.props.ExplicitPartitions {
		s.writeResponse(&ReleaseResp{
			Generation: genID,
			Topic:      topicName,
			Partition:  partition,
			MustCommit: mustCommit,
		})
	}
}

func (s *Session) writeResponse(resp Response) {
	select {
	case s.responses <- resp:
		s.logger.Log(s.ctx, log.LevelDebug, "response scheduled to write", map[string]interface{}{
			"response": resp,
		})
	case <-s.ctx.Done():
		s.logger.Log(s.ctx, log.LevelWarn, "failed to write response: session cancelled", map[string]interface{}{
			"response": resp,
		})
	}
}

var _ topic.ReadSessionDispatcher = &readDispatcher{}

type readDispatcher Session

func (r *readDispatcher) LockPartition(genID uint64, p *model.PartitionInfo) {
	(*Session)(r).lockPartition(genID, p)
}

func (r *readDispatcher) ReleasePartition(genID uint64, topicName string, partition uint32, mustCommit bool) {
	(*Session)(r).releasePartition(genID, topicName, partition, mustCommit)
}

func (r *readDispatcher) ReadSignal() chan<- struct{} {
	return r.readSignal
}
