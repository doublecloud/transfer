package yccreds

import (
	"context"

	"github.com/doublecloud/tross/cloud/bitbucket/public-api/yandex/cloud/iam/v1"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/yc"
	"github.com/golang/protobuf/ptypes/timestamp"
)

type Credentials interface {
	Token(context.Context) (string, error)
	ExpiresAt() *timestamp.Timestamp
}

type sdkCredentials struct {
	creds Credentials
}

func (s sdkCredentials) IAMToken(ctx context.Context) (*iam.CreateIamTokenResponse, error) {
	token, err := s.creds.Token(ctx)
	if err != nil {
		return nil, err
	}
	return &iam.CreateIamTokenResponse{
		IamToken:  token,
		ExpiresAt: s.creds.ExpiresAt(),
	}, nil
}

func (s sdkCredentials) YandexCloudAPICredentials() {
}

func NewSdkCredentials(creds Credentials) yc.NonExchangeableCredentials {
	return &sdkCredentials{creds: creds}
}
