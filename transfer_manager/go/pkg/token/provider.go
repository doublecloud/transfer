package token

import (
	"context"
	"crypto/tls"

	"github.com/doublecloud/tross/cloud/bitbucket/private-api/yandex/cloud/priv/iam/v1"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/cleanup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Provider interface {
	cleanup.Closeable
	CreateToken(ctx context.Context, oauthToken string) (string, error)
}

type ClientConfig struct {
	Endpoint string `mapstructure:"endpoint"`
}

type ProviderConfig struct {
	Client ClientConfig `mapstructure:"client"`
}

type provider struct {
	client iam.IamTokenServiceClient
	conn   *grpc.ClientConn
}

func NewProvider(ctx context.Context, config *ProviderConfig) (Provider, error) {
	client, conn, err := newClient(ctx, &config.Client)
	if err != nil {
		return nil, xerrors.Errorf("unable to initialize client: %w", err)
	}
	return &provider{client: client, conn: conn}, nil
}

func (p *provider) Close() error {
	return p.conn.Close()
}

func (p *provider) CreateToken(ctx context.Context, oauthToken string) (string, error) {
	request := &iam.CreateIamTokenRequest{
		Identity: &iam.CreateIamTokenRequest_YandexPassportOauthToken{
			YandexPassportOauthToken: oauthToken,
		},
	}
	response, err := p.client.Create(ctx, request)
	if err != nil {
		return "", err
	}
	return response.IamToken, nil
}

func newClient(ctx context.Context, config *ClientConfig) (iam.IamTokenServiceClient, *grpc.ClientConn, error) {
	creds := credentials.NewTLS(new(tls.Config))
	conn, err := grpc.DialContext(ctx, config.Endpoint, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, nil, xerrors.Errorf("unable to connect: %w", err)
	}
	client := iam.NewIamTokenServiceClient(conn)
	return client, conn, nil
}
