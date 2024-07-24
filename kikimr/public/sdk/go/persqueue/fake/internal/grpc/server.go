// Copyright (c) 2019 Yandex LLC. All rights reserved.
// Author: Andrey Khaliullin <avhaliullin@yandex-team.ru>

package grpc

import (
	"context"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/fake/data"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/fake/internal/broker"
	logbroker "github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/genproto/Ydb_PersQueue_V0"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/log"
)

// Server is grpc handler supporting lb's read/write session and choose proxy methods
type Server struct {
	logbroker.PersQueueServiceServer
	b       *broker.Broker
	log     log.Logger
	address string
}

func NewServer(ctx context.Context, log log.Logger, address string) *Server {
	return &Server{
		log:     log,
		b:       broker.NewBroker(ctx, log),
		address: address,
	}
}

func (s *Server) WriteSession(session logbroker.PersQueueService_WriteSessionServer) error {
	return s.handleWrite(session)
}

func (s *Server) ReadSession(session logbroker.PersQueueService_ReadSessionServer) error {
	return s.handleRead(session)
}

// fake methods

func (s *Server) Address() string {
	return s.address
}

func (s *Server) Subscribe(ctx context.Context, topic string, ch chan<- *data.Message) error {
	t, err := s.b.GetTopic(topic)
	if err != nil {
		return err
	}
	for _, p := range t.GetParts() {
		p.ReadAsync(ctx, 0, ch)
	}
	return nil
}

func (s *Server) CreateTopic(name string, parts int) error {
	return s.b.CreateTopic(name, parts)
}

type sessionHandler struct {
	sessionID string
	server    *Server
	log       log.Logger
}
