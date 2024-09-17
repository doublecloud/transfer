package ydb

import (
	"context"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/credentials"
	"github.com/doublecloud/transfer/pkg/providers/ydb/logadapter"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"github.com/ydb-platform/ydb-go-yc-metadata"
)

type ydbCredsSdkV3 struct {
	database string
	instance string
	opts     []ydb.Option
	isSecure bool
}

func ydbSourceToCreds(cfg *YdbSource) (*ydbCredsSdkV3, error) {
	if cfg.SAKeyContent != "" {
		return nil, abstract.NewFatalError(xerrors.New("SAKeyContent is not supported for now"))
	}

	isSecure := false
	opts := make([]ydb.Option, 0)

	if cfg.TLSEnabled {
		isSecure = true
		opts = append(opts, yc.WithInternalCA())
	}
	if cfg.ServiceAccountID != "" {
		creds, err := credentials.NewServiceAccountCreds(logger.Log, cfg.ServiceAccountID)
		if err != nil {
			return nil, xerrors.Errorf("could not create service account YDB credentials: %w", err)
		}
		opts = append(opts, ydb.WithCredentials(creds))
	} else {
		opts = append(opts, ydb.WithAccessTokenCredentials(string(cfg.Token)))
	}

	return &ydbCredsSdkV3{
		database: cfg.Database,
		instance: cfg.Instance,
		opts:     opts,
		isSecure: isSecure,
	}, nil
}

func newClient2(ctx context.Context, cfg *YdbSource) (*ydb.Driver, error) {
	creds, err := ydbSourceToCreds(cfg)
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve creds: %w", err)
	}

	options := make([]ydb.Option, len(creds.opts), len(creds.opts)+1)
	copy(options, creds.opts)
	if cfg.VerboseSDKLogs {
		options = append(options, logadapter.WithTraces(logger.Log, trace.DetailsAll))
	}

	db, err := ydb.Open(ctx, sugar.DSN(creds.instance, creds.database, creds.isSecure), options...)
	if err != nil {
		return nil, xerrors.Errorf("unable to create ydb client, err: %w", err)
	}

	return db, nil
}
