// Copyright (c) 2019 Yandex LLC. All rights reserved.
// Author: Andrey Khaliullin <avhaliullin@yandex-team.ru>

package grpc

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/fake/internal/consumer"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/fake/internal/model"
	logbroker "github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/genproto/Ydb_PersQueue_V0"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/log"
)

const (
	respsQueue           = 10
	defaultMessagesLimit = 1024
)

func (s *Server) handleRead(session logbroker.PersQueueService_ReadSessionServer) error {
	sessionID := fmt.Sprintf("%d", rand.Uint32())
	ctx, cancel := context.WithCancel(session.Context())
	h := &readHandler{
		sessionHandler: sessionHandler{
			sessionID: sessionID,
			server:    s,
			log:       s.log.With("sessionID", sessionID),
		},
		ctx:       ctx,
		cancel:    cancel,
		session:   session,
		responses: make(chan consumer.Response, respsQueue),
	}
	err := h.handleInitReq()
	if err != nil {
		return err
	}
	h.log.Log(context.Background(), log.LevelInfo, "started fake lb read session", nil)
	go h.readLoop()
	return h.writeLoop()
}

// readHandler is thin bridge between grpc
type readHandler struct {
	sessionHandler
	ctx       context.Context
	cancel    context.CancelFunc
	session   logbroker.PersQueueService_ReadSessionServer
	consumer  *consumer.Session
	responses chan consumer.Response
}

func (h *readHandler) handleInitReq() error {
	req, err := h.session.Recv()
	if err != nil {
		return err
	}
	initReq, ok := req.Request.(*logbroker.ReadRequest_Init_)
	if !ok {
		return fmt.Errorf("init request expected, found %v", req.Request)
	}
	init := initReq.Init
	props := &consumer.ReadSessionProps{
		Topics:              init.Topics,
		SessionID:           h.sessionID,
		ClientID:            init.ClientId,
		ExplicitPartitions:  init.ClientsideLocksAllowed,
		WaitCommitOnRelease: !init.BalancePartitionRightNow,
	}

	h.consumer, err = consumer.NewSession(
		h.ctx,
		h.log,
		h.server.b,
		props,
		h.responses,
	)
	if err != nil {
		return err
	}

	err = h.session.Send(&logbroker.ReadResponse{
		Response: &logbroker.ReadResponse_Init_{
			Init: &logbroker.ReadResponse_Init{
				SessionId: props.SessionID,
			},
		},
	})
	if err != nil {
		return err
	}
	return nil
}

func (h *readHandler) readLoop() {
	defer h.cancel()
	defer h.log.Log(h.ctx, log.LevelInfo, "leaving fake lb read session", nil)

	for h.ctx.Err() == nil {
		req, err := h.session.Recv()
		if err != nil {
			h.log.Log(h.ctx, log.LevelError, "failed to read message", map[string]interface{}{
				"error": err.Error(),
			})
			return
		}
		h.log.Log(h.ctx, log.LevelDebug, "got request", map[string]interface{}{
			"request": req,
		})
		var modelReq consumer.Request
		switch r := req.Request.(type) {
		case *logbroker.ReadRequest_Read_:
			modelReq, err = h.parseRead(r.Read)
		case *logbroker.ReadRequest_Commit_:
			modelReq, err = h.parseCommit(r.Commit)
		case *logbroker.ReadRequest_StartRead_:
			modelReq, err = h.parseStartRead(r.StartRead)
		default:
			err = fmt.Errorf("unsupported request %v", req)
		}
		if err != nil {
			h.log.Log(h.ctx, log.LevelError, "failed to parse message", map[string]interface{}{
				"message": req.Request,
				"error":   err,
			})
			return
		}
		select {
		case <-h.ctx.Done():
		case h.consumer.Requests() <- modelReq:
		}
	}
}

func (h *readHandler) parseStartRead(req *logbroker.ReadRequest_StartRead) (*consumer.StartReadReq, error) {
	return &consumer.StartReadReq{
		Generation:   req.Generation,
		ReadOffset:   req.ReadOffset,
		CommitOffset: req.CommitOffset,
		VerifyOffset: req.VerifyReadOffset,
		Topic:        req.Topic,
		Partition:    req.Partition,
	}, nil
}

func (h *readHandler) parseCommit(req *logbroker.ReadRequest_Commit) (*consumer.CommitReq, error) {
	var cookies []int
	for _, cookie := range req.Cookie {
		cookies = append(cookies, int(cookie))
	}
	return &consumer.CommitReq{
		Cookies: cookies,
	}, nil
}

func (h *readHandler) parseRead(req *logbroker.ReadRequest_Read) (*consumer.ReadReq, error) {
	maxMessages := int(req.MaxCount)
	if maxMessages == 0 {
		maxMessages = defaultMessagesLimit
	}
	return &consumer.ReadReq{
		MaxMessages: maxMessages,
	}, nil
}

func (h *readHandler) writeLoop() error {
	defer h.log.Log(h.ctx, log.LevelInfo, "exiting read session", nil)

	for h.ctx.Err() == nil {
		select {
		case <-h.ctx.Done():
			return h.ctx.Err()
		case <-h.consumer.ClosedCh():
			return fmt.Errorf("consumer closed")
		case resp := <-h.responses:
			var protoResp *logbroker.ReadResponse
			switch modelResp := resp.(type) {
			case *consumer.MessagesResp:
				protoResp = messages2Proto(modelResp)
			case *consumer.CommitResp:
				protoResp = commit2Proto(modelResp)
			case *consumer.LockResp:
				protoResp = lock2Proto(modelResp)
			case *consumer.ReleaseResp:
				protoResp = release2Proto(modelResp)
			default:
				panic(fmt.Sprintf("unknown response %v", resp))
			}
			h.log.Log(h.ctx, log.LevelDebug, "sending response", map[string]interface{}{
				"response": protoResp,
			})
			err := h.session.Send(protoResp)
			if err != nil {
				return err
			}
		}
	}
	return h.ctx.Err()
}

func messages2Proto(resp *consumer.MessagesResp) *logbroker.ReadResponse {
	return &logbroker.ReadResponse{Response: &logbroker.ReadResponse_BatchedData_{BatchedData: &logbroker.ReadResponse_BatchedData{
		Cookie:        uint64(resp.Cookie),
		PartitionData: model.MessagesToProto(resp.Messages),
	}}}
}

func commit2Proto(resp *consumer.CommitResp) *logbroker.ReadResponse {
	cookies := make([]uint64, len(resp.Cookies))
	for idx, c := range resp.Cookies {
		cookies[idx] = uint64(c)
	}
	return &logbroker.ReadResponse{Response: &logbroker.ReadResponse_Commit_{Commit: &logbroker.ReadResponse_Commit{
		Cookie: cookies,
	}}}
}

func lock2Proto(resp *consumer.LockResp) *logbroker.ReadResponse {
	return &logbroker.ReadResponse{Response: &logbroker.ReadResponse_Lock_{Lock: &logbroker.ReadResponse_Lock{
		Topic:      resp.Partition.Topic,
		Partition:  resp.Partition.Partition,
		ReadOffset: resp.Partition.ReadOffset,
		EndOffset:  resp.Partition.EndOffset,
		Generation: resp.Generation,
	}}}
}

func release2Proto(resp *consumer.ReleaseResp) *logbroker.ReadResponse {
	return &logbroker.ReadResponse{Response: &logbroker.ReadResponse_Release_{Release: &logbroker.ReadResponse_Release{
		Topic:      resp.Topic,
		Partition:  resp.Partition,
		CanCommit:  resp.MustCommit,
		Generation: resp.Generation,
	}}}
}
