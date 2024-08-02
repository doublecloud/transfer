package yccreds

import (
	"context"
	"sync"

	"github.com/doublecloud/tross/library/go/core/log"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/golang/protobuf/ptypes/timestamp"
)

var (
	globalCreds *globalIamCreds
)

type globalIamCreds struct {
	credentials Credentials
	mutex       sync.Mutex
}

func (c *globalIamCreds) ExpiresAt() *timestamp.Timestamp {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.credentials.ExpiresAt()
}

func (c *globalIamCreds) Token(ctx context.Context) (string, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.credentials.Token(ctx)
}

func Initialize(logger log.Logger) (err error) {
	if globalCreds != nil {
		return xerrors.New("creds has been initialized")
	}
	iam, err := NewIamCreds(logger)
	if err != nil {
		return xerrors.Errorf("cannot init iam creds: %w", err)
	}
	globalCreds = &globalIamCreds{
		iam,
		sync.Mutex{}}
	return nil
}

func Instance() (*globalIamCreds, error) {
	if globalCreds != nil {
		return globalCreds, nil
	}
	return nil, xerrors.New("creds has not been initialized")
}
