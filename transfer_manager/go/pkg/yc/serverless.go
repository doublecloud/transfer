package yc

import (
	"context"

	"github.com/doublecloud/tross/cloud/bitbucket/public-api/yandex/cloud/serverless/functions/v1"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"google.golang.org/grpc"
)

type Serverless struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func (s *Serverless) ListFunctions(ctx context.Context, in *functions.ListFunctionsRequest) (*functions.ListFunctionsResponse, error) {
	conn, err := s.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return functions.NewFunctionServiceClient(conn).List(ctx, in)
}
