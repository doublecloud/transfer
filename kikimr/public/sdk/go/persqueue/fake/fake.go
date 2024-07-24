// Copyright (c) 2019 Yandex LLC. All rights reserved.
// Author: Andrey Khaliullin <avhaliullin@yandex-team.ru>

package fake

import (
	"context"
	"fmt"
	"net"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/fake/data"
	lbgrpc "github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/fake/internal/grpc"
	logbroker "github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/genproto/Ydb_PersQueue_V0"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/log"
	"google.golang.org/grpc"
)

// NewFake starts logbroker fake and returns it's address with port
func NewFake(ctx context.Context, logger log.Logger) (LBFake, error) {
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return nil, err
	}
	port := listener.Addr().(*net.TCPAddr).Port

	lbServer := lbgrpc.NewServer(ctx, logger, fmt.Sprintf("localhost:%d", port))
	grpcServer := grpc.NewServer()
	logbroker.RegisterPersQueueServiceServer(grpcServer, lbServer)

	go func() {
		logger.Log(ctx, log.LevelInfo, "Started LB fake", map[string]interface{}{
			"port": port,
		})
		err := grpcServer.Serve(listener)
		if err != nil {
			logger.Log(ctx, log.LevelError, "Server exited", map[string]interface{}{
				"error": err.Error(),
			})
		}
	}()
	go func() {
		<-ctx.Done()
		_ = listener.Close()
		grpcServer.Stop()
	}()
	return lbServer, nil
}

type LBFake interface {
	Address() string
	Subscribe(ctx context.Context, topic string, ch chan<- *data.Message) error
	CreateTopic(name string, parts int) error
}
