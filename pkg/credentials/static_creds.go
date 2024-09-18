package credentials

import (
	"context"

	"github.com/golang/protobuf/ptypes/timestamp"
)

type StaticCreds struct {
	AuthToken string
}

func (c *StaticCreds) ExpiresAt() *timestamp.Timestamp {
	return nil
}

func (c *StaticCreds) Token(context.Context) (string, error) {
	return c.AuthToken, nil
}

func NewStaticCreds(token string) Credentials {
	return &StaticCreds{AuthToken: token}
}
