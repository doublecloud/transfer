package yccreds

import (
	"context"
	"time"

	"github.com/doublecloud/tross/library/go/core/log"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/yc"
	ycsdk "github.com/doublecloud/tross/transfer_manager/go/pkg/yc/sdk"
	"github.com/golang/protobuf/ptypes/timestamp"
)

type iamCreds struct {
	sdk     *yc.SDK
	logger  log.Logger
	expires time.Time
	token   string
}

func (c *iamCreds) ExpiresAt() *timestamp.Timestamp {
	return &timestamp.Timestamp{
		Seconds: c.expires.Unix(),
		Nanos:   0,
	}
}

func (c *iamCreds) Token(ctx context.Context) (string, error) {
	if c.token != "" && !c.expires.IsZero() && !c.expires.Before(time.Now()) {
		return c.token, nil
	}
	iamToken, err := c.sdk.CreateIAMToken(ctx)
	if err != nil {
		if c.logger != nil {
			c.logger.Warn("credentials: unable to create token", log.Error(err))
		}
		return "", err
	}
	c.token = iamToken.IamToken
	c.expires = time.Unix(iamToken.ExpiresAt.Seconds, int64(iamToken.ExpiresAt.Nanos))
	if c.logger != nil {
		c.logger.Info("credentials: got new iam token that expires at", log.Any("expires", c.expires))
	}
	return c.token, nil
}

func NewIamCreds(logger log.Logger) (Credentials, error) {
	sdk, err := ycsdk.Instance()
	if err != nil {
		return nil, xerrors.Errorf("Cannot init creds without YC SDK: %w", err)
	}
	return &iamCreds{
		sdk:     sdk,
		logger:  logger,
		expires: time.Time{},
		token:   "",
	}, nil
}
