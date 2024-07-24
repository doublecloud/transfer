package connect

import (
	"context"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/scheme"
)

type schemeWrapper struct {
	ctx    context.Context
	client *scheme.Client
}

func newSchemeWrapper(ctx context.Context) *schemeWrapper {
	return &schemeWrapper{
		ctx:    ctx,
		client: &scheme.Client{},
	}
}
