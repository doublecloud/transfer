package connect

import (
	"context"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/table"
)

type tableWrapper struct {
	ctx         context.Context
	client      *table.Client
	sessionPool *table.SessionPool
}

func newTableWrapper(ctx context.Context) *tableWrapper {
	tableClient := &table.Client{
		Trace: table.ContextClientTrace(ctx),
	}
	return &tableWrapper{
		ctx:    ctx,
		client: tableClient,
		sessionPool: &table.SessionPool{
			Builder: tableClient,
			Trace:   table.ContextSessionPoolTrace(ctx),
		},
	}
}

func (t *tableWrapper) CreateSession(ctx context.Context) (*table.Session, error) {
	return t.client.CreateSession(ctx)
}

func (t *tableWrapper) Pool() *table.SessionPool {
	return t.sessionPool
}
