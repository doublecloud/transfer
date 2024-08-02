package yccreds

import (
	"context"
	"time"

	"github.com/doublecloud/tross/cloud/bitbucket/public-api/yandex/cloud/iam/v1"
	"github.com/doublecloud/tross/library/go/core/log"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/yc"
	ycsdk "github.com/doublecloud/tross/transfer_manager/go/pkg/yc/sdk"
	"github.com/golang/protobuf/ptypes/timestamp"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type serviceAccountCreds struct {
	sdk              *yc.SDK
	logger           log.Logger
	serviceAccountID string
	creds            Credentials
	expires          time.Time
	token            string
}

func (c *serviceAccountCreds) ExpiresAt() *timestamp.Timestamp {
	return &timestamp.Timestamp{Seconds: c.expires.Unix()}
}

func (c *serviceAccountCreds) Token(ctx context.Context) (string, error) {
	if c.token != "" && !c.expires.IsZero() && !c.expires.Before(time.Now()) {
		return c.token, nil
	}
	ownToken, err := c.creds.Token(ctx)
	if err != nil {
		return "", xerrors.Errorf("credentials: unable to create parent token: %w", err)
	}
	getSATokenCtx := metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{
		"authorization": "Bearer " + ownToken,
	}))
	iamToken, err := c.sdk.IAM().IamToken().CreateForServiceAccount(getSATokenCtx, &iam.CreateIamTokenForServiceAccountRequest{
		ServiceAccountId: c.serviceAccountID,
	})
	if err != nil {
		var details []interface{}
		if s, ok := status.FromError(err); ok {
			details = s.Details()
		}
		return "", xerrors.Errorf("credentials: unable to create SA iam token, details: %v, error: %w", details, err)
	}
	c.token = iamToken.IamToken
	c.expires = time.Unix(iamToken.ExpiresAt.Seconds, int64(iamToken.ExpiresAt.Nanos))
	if c.logger != nil {
		c.logger.Info("credentials: got new iam token for SA that expires at", log.Any("expires", c.expires))
	}
	return c.token, nil
}

func NewServiceAccountCreds(logger log.Logger, serviceAccountID string) (Credentials, error) {
	sdk, err := ycsdk.Instance()
	if err != nil {
		return nil, xerrors.Errorf("Cannot init creds without YC SDK: %w", err)
	}
	refreshableTokenCreds, err := NewIamCreds(logger)
	if err != nil {
		return nil, err
	}
	return &serviceAccountCreds{
		sdk:              sdk,
		logger:           logger,
		serviceAccountID: serviceAccountID,
		creds:            refreshableTokenCreds,
		expires:          refreshableTokenCreds.ExpiresAt().AsTime(),
		token:            "",
	}, nil
}
