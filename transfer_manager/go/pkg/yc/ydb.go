package yc

import (
	"context"

	ydbpb "github.com/doublecloud/tross/cloud/bitbucket/public-api/yandex/cloud/ydb/v1"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"google.golang.org/grpc"
)

type YDB struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func (c *YDB) List(ctx context.Context, in *ydbpb.ListDatabasesRequest, opts ...grpc.CallOption) (*ydbpb.ListDatabasesResponse, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return ydbpb.NewDatabaseServiceClient(conn).List(ctx, in, opts...)
}

func (c *YDB) GetDatabase(ctx context.Context, in *ydbpb.GetDatabaseRequest, opts ...grpc.CallOption) (*ydbpb.Database, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return ydbpb.NewDatabaseServiceClient(conn).Get(ctx, in, opts...)
}
