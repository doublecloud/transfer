package ydb

import (
	"context"
	"fmt"

	"google.golang.org/grpc/metadata"
)

const (
	metaDatabase    = "x-ydb-database"
	metaTicket      = "x-ydb-auth-ticket"
	metaVersion     = "x-ydb-sdk-build-info"
	metaRequestType = "x-ydb-request-type"
	metaTraceID     = "x-ydb-trace-id"
	metaUserAgent   = "x-ydb-user-agent"
)

type meta struct {
	trace        DriverTrace
	credentials  Credentials
	database     string
	requestsType string
}

type MetaDatabaseCtxKey struct{}

func (m *meta) md(ctx context.Context) (md metadata.MD, err error) {
	var has bool
	if md, has = metadata.FromOutgoingContext(ctx); !has {
		md = metadata.MD{}
	}
	database := m.database
	if v, ok := ctx.Value(MetaDatabaseCtxKey{}).(string); ok {
		database = v
	}
	md.Set(metaDatabase, database)
	md.Set(metaVersion, Version)
	if m.requestsType != "" {
		md.Set(metaRequestType, m.requestsType)
	}
	if m.credentials != nil {
		var token string
		driverTraceGetCredentialsDone := driverTraceOnGetCredentials(ctx, m.trace, ctx)
		defer func() {
			driverTraceGetCredentialsDone(ctx, token != "", err)
		}()
		token, err = m.credentials.Token(ctx)
		if err != nil {
			if stringer, ok := m.credentials.(fmt.Stringer); ok {
				return nil, fmt.Errorf("%s: %w", stringer.String(), err)
			}
			return nil, err
		}
		md.Set(metaTicket, token)
	}
	return md, nil
}
