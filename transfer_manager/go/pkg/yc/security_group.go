package yc

import (
	"context"

	"github.com/doublecloud/tross/cloud/bitbucket/public-api/yandex/cloud/vpc/v1"
	"google.golang.org/grpc"
)

func NewSecurityGroup(g func(ctx context.Context) (*grpc.ClientConn, error), ctx context.Context) vpc.SecurityGroupServiceClient {
	conn, err := g(ctx)
	if err != nil {
		return nil
	}
	return vpc.NewSecurityGroupServiceClient(conn)
}

func (s *VPC) SecurityGroup(ctx context.Context) vpc.SecurityGroupServiceClient {
	return NewSecurityGroup(s.getConn, ctx)
}
