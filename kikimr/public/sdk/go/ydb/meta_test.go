package ydb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestMetaRequiredHeaders(t *testing.T) {
	m := &meta{
		database:     "database",
		requestsType: "requestType",
		credentials: CredentialsFunc(func(context.Context) (string, error) {
			return "token", nil
		}),
	}

	ctx := context.Background()

	ctx = WithUserAgent(ctx, "userAgent")

	ctx = WithTraceID(ctx, "traceID")

	ctx = metadata.AppendToOutgoingContext(ctx, "some-user-header", "some-user-value")

	md, err := m.md(ctx)
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, []string{"database"}, md.Get(metaDatabase))
	require.Equal(t, []string{"requestType"}, md.Get(metaRequestType))
	require.Equal(t, []string{"token"}, md.Get(metaTicket))
	require.Equal(t, []string{"userAgent"}, md.Get(metaUserAgent))
	require.Equal(t, []string{"traceID"}, md.Get(metaTraceID))
	require.Equal(t, []string{Version}, md.Get(metaVersion))
	require.Equal(t, []string{"some-user-value"}, md.Get("some-user-header"))
}
