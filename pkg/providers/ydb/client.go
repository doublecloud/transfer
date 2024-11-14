package ydb

import (
	"context"
	"crypto/tls"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/providers/ydb/logadapter"
	ydb3 "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func NewYDBDriver(ctx context.Context, database, instance string, credentials credentials.Credentials, tlsConfig *tls.Config) (*ydb3.Driver, error) {
	secure := tlsConfig != nil
	driver, err := ydb3.Open(
		ctx,
		sugar.DSN(instance, database, sugar.WithSecure(secure)),
		ydb3.WithCredentials(credentials),
		ydb3.WithTLSConfig(tlsConfig),
		logadapter.WithTraces(logger.Log, trace.DriverEvents),
	)
	if err != nil {
		return nil, xerrors.Errorf("YDB open error: %w", err)
	}

	return driver, nil
}
