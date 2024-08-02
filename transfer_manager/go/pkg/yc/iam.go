package yc

import (
	"context"

	privateiam "github.com/doublecloud/tross/cloud/bitbucket/private-api/yandex/cloud/priv/iam/v1"
	"github.com/doublecloud/tross/cloud/bitbucket/public-api/yandex/cloud/iam/v1"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/util"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// CreateIamTokenResponse is minimal interface for token
// It is implemented by both public and private iam.CreateIamTokenResponse
type CreateIamTokenResponse interface {
	GetIamToken() string
	GetExpiresAt() *timestamppb.Timestamp
}

// IamTokenServiceClient is a iam.IamTokenServiceClient with
// lazy GRPC connection initialization.
type IamTokenServiceClient struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

// Create implements iam.IamTokenServiceClient
func (c *IamTokenServiceClient) Create(ctx context.Context, in *iam.CreateIamTokenRequest, opts ...grpc.CallOption) (*iam.CreateIamTokenResponse, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return iam.NewIamTokenServiceClient(conn).Create(ctx, in, opts...)
}

// CreateForServiceAccount implements iam.IamTokenServiceClient
func (c *IamTokenServiceClient) CreateForServiceAccount(ctx context.Context, in *iam.CreateIamTokenForServiceAccountRequest) (*iam.CreateIamTokenResponse, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return iam.NewIamTokenServiceClient(conn).CreateForServiceAccount(ctx, in)
}

type PrivateIamTokenServiceClient struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func (c *PrivateIamTokenServiceClient) Create(ctx context.Context, in *iam.CreateIamTokenRequest, opts ...grpc.CallOption) (*privateiam.CreateIamTokenResponse, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	privateReq := &privateiam.CreateIamTokenRequest{}
	if err := util.MapProtoJSON(in, privateReq); err != nil {
		return nil, xerrors.Errorf("cannot create iam token request from JSON: %w", err)
	}
	return privateiam.NewIamTokenServiceClient(conn).Create(ctx, privateReq, opts...)
}
