package credentials

import (
	"context"

	"github.com/doublecloud/tross/library/go/core/log"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/golang/protobuf/ptypes/timestamp"
)

type Credentials interface {
	Token(context.Context) (string, error)
	ExpiresAt() *timestamp.Timestamp
}

var NewServiceAccountCreds = func(logger log.Logger, serviceAccountID string) (Credentials, error) {
	return nil, xerrors.New("not implemented")
}

var NewIamCreds = func(logger log.Logger) (Credentials, error) {
	return nil, xerrors.Errorf("not implemented")
}
