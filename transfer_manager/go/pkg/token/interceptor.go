package token

import (
	"context"

	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/grpcutil"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/stringutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type InterceptorConfig struct {
	Skip               func(info *grpc.UnaryServerInfo) bool
	AuthorizationMDKey string
	OAuthTokenPrefix   []string
	IamTokenPrefix     []string
}

func NewInterceptorConfig(skip func(info *grpc.UnaryServerInfo) bool) *InterceptorConfig {
	return &InterceptorConfig{
		Skip:               skip,
		AuthorizationMDKey: "authorization",
		OAuthTokenPrefix:   []string{"oauth "},
		IamTokenPrefix:     []string{"iam ", "bearer "},
	}
}

type interceptor struct {
	config   *InterceptorConfig
	provider Provider
}

func NewInterceptor(config *InterceptorConfig, provider Provider) grpcutil.UnaryServerInterceptor {
	return &interceptor{config: config, provider: provider}
}

func (i *interceptor) Intercept(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if i.config.Skip == nil || !i.config.Skip(info) {
		var err error
		ctx, err = i.intercept(ctx)
		if err != nil {
			return nil, status.Errorf(codes.Unauthenticated, "interception failed: %v", err)
		}
	}

	return handler(ctx, req)
}

func (i *interceptor) intercept(ctx context.Context) (context.Context, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, xerrors.New("unable to get incoming metadata")
	}

	authorizationMD, ok := md[i.config.AuthorizationMDKey]
	if !ok {
		return nil, xerrors.New("unable to get authorization metadata")
	}
	if len(authorizationMD) == 0 {
		return nil, xerrors.New("authorization metadata is empty")
	}
	authorization := authorizationMD[0]

	token, ok := stringutil.TrimPrefixCI(authorization, i.config.IamTokenPrefix...)
	if !ok {
		oauthToken, ok := stringutil.TrimPrefixCI(authorization, i.config.OAuthTokenPrefix...)
		if !ok {
			oauthToken = authorization
		}

		var err error
		token, err = i.provider.CreateToken(ctx, oauthToken)
		if err != nil {
			return nil, xerrors.Errorf("unable to create token: %w", err)
		}
	}

	ctx = WithToken(ctx, token)
	return ctx, nil
}
