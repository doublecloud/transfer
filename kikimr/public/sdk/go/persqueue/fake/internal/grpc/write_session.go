// Copyright (c) 2019 Yandex LLC. All rights reserved.
// Author: Andrey Khaliullin <avhaliullin@yandex-team.ru>

package grpc

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/fake/data"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/fake/internal/topic"
	logbroker "github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/genproto/Ydb_PersQueue_V0"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/log"
)

const (
	respChanSize = 64
)

func (s *Server) handleWrite(session logbroker.PersQueueService_WriteSessionServer) error {
	sessionID := fmt.Sprintf("%d", rand.Uint32())
	h := &writeHandler{
		sessionHandler: sessionHandler{
			sessionID: sessionID,
			server:    s,
			log:       s.log.With("sessionID", sessionID),
		},
		session:  session,
		respChan: make(chan *topic.ResponseAck, respChanSize),
	}
	err := h.handleInitReq()
	if err != nil {
		return err
	}
	h.log.Log(h.ctx(), log.LevelInfo, "started fake lb write session", nil)
	go h.writeLoop()
	return h.readLoop()
}

type writeHandler struct {
	sessionHandler
	session  logbroker.PersQueueService_WriteSessionServer
	writer   *topic.Writer
	respChan chan *topic.ResponseAck
}

func (h *writeHandler) ctx() context.Context {
	return h.session.Context()
}

func (h *writeHandler) readLoop() error {
	defer h.log.Log(h.ctx(), log.LevelInfo, "leaving fake lb write session", nil)
	for h.ctx().Err() == nil {
		req, err := h.session.Recv()
		if err != nil {
			return err
		}
		h.log.Log(h.ctx(), log.LevelDebug, "got request", map[string]interface{}{
			"request": req,
		})
		switch r := req.Request.(type) {
		case *logbroker.WriteRequest_DataBatch_:
			h.handleWriteBatchReq(r.DataBatch)
		case *logbroker.WriteRequest_Data_:
			h.handleWriteReq(r.Data)
		default:
			return fmt.Errorf("unsupported request: %v", r)
		}
	}
	return h.ctx().Err()
}

func (h *writeHandler) handleWriteReq(req *logbroker.WriteRequest_Data) {
	h.handleWriteBatchReq(&logbroker.WriteRequest_DataBatch{Data: []*logbroker.WriteRequest_Data{req}})
}

func (h *writeHandler) handleWriteBatchReq(req *logbroker.WriteRequest_DataBatch) {
	writeReq := &topic.WriteReq{
		ResponseCh: h.respChan,
	}
	for _, d := range req.Data {
		writeReq.Messages = append(writeReq.Messages, &data.WriteMessage{
			Data:    d.Data,
			SeqNo:   d.SeqNo,
			Created: time.Unix(0, int64(d.CreateTimeMs)*int64(time.Millisecond)),
		})
	}
	select {
	case <-h.ctx().Done():
	case h.writer.RequestsChan() <- writeReq:
	}
}

func (h *writeHandler) writeLoop() {
	for h.ctx().Err() == nil {
		select {
		case <-h.ctx().Done():
			return
		case resp := <-h.respChan:
			err := h.handleResp(resp)
			if err != nil {
				h.log.Log(h.ctx(), log.LevelError, "writing error", map[string]interface{}{
					"error": err.Error(),
				})
				return
			}
		}
	}
}

func (h *writeHandler) handleResp(resp *topic.ResponseAck) error {
	acks := make([]*logbroker.WriteResponse_Ack, len(resp.Acks))
	for i, ack := range resp.Acks {
		acks[i] = &logbroker.WriteResponse_Ack{
			Offset:         uint64(ack.Offset),
			SeqNo:          ack.SeqNo,
			AlreadyWritten: ack.AlreadyWritten,
			Stat:           &logbroker.WriteResponse_Stat{},
		}
	}
	var lbMsg logbroker.WriteResponse
	if len(acks) == 1 {
		lbMsg.Response = &logbroker.WriteResponse_Ack_{
			Ack: acks[0],
		}
	} else {
		lbMsg.Response = &logbroker.WriteResponse_AckBatch_{
			AckBatch: &logbroker.WriteResponse_AckBatch{
				Ack: acks,
			},
		}
	}
	return h.session.Send(&lbMsg)
}

func (h *writeHandler) handleInitReq() error {
	req, err := h.session.Recv()
	if err != nil {
		return err
	}
	initReq, ok := req.Request.(*logbroker.WriteRequest_Init_)
	if !ok {
		return fmt.Errorf("init request expected, found %v", req.Request)
	}
	init := initReq.Init
	topicName := init.Topic
	source := init.SourceId
	h.writer, err = h.server.b.GetTopicWriter(topicName, string(source))
	if err != nil {
		return err
	}
	return h.session.Send(&logbroker.WriteResponse{
		Response: &logbroker.WriteResponse_Init_{
			Init: &logbroker.WriteResponse_Init{
				Topic:     topicName,
				SessionId: h.sessionID,
				Partition: 0,
				MaxSeqNo:  h.writer.MaxSeqNo(),
			},
		},
	})
}
