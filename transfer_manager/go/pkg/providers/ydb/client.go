package ydb

import (
	"context"
	"crypto/tls"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	ydbLog "github.com/doublecloud/transfer/library/go/yandex/ydb/log"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	ydb3 "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type YDBClient struct {
	Driver   ydb.Driver
	Database string
	Instance string
}

func NewYDBClient(ctx context.Context, database, instance string, credentials ydb.Credentials, tlsConfig *tls.Config) (*YDBClient, error) {
	trace := NewLogTrace(logger.Log)
	dialer := &ydb.Dialer{
		DriverConfig: &ydb.DriverConfig{
			Database:    database,
			Credentials: credentials,
			Trace:       trace.YDBTrace(),
		},
	}
	dialer.TLSConfig = tlsConfig
	driver, err := dialer.Dial(ctx, instance)
	if err != nil {
		return nil, xerrors.Errorf("YDB dial error: %w", err)
	}
	return &YDBClient{Driver: driver, Database: database, Instance: instance}, nil
}

func NewYDBDriver(ctx context.Context, database, instance string, credentials credentials.Credentials, tlsConfig *tls.Config) (*ydb3.Driver, error) {
	secure := tlsConfig != nil
	driver, err := ydb3.Open(
		ctx,
		sugar.DSN(instance, database, secure),
		ydb3.WithCredentials(credentials),
		ydb3.WithTLSConfig(tlsConfig),
		ydbLog.WithTraces(logger.Log, trace.DriverEvents),
	)
	if err != nil {
		return nil, xerrors.Errorf("YDB open error: %w", err)
	}

	return driver, nil
}
