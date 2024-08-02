package grpcutil

import (
	"context"

	"google.golang.org/grpc"
)

type UnaryServerInterceptor interface {
	Intercept(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error)
}
