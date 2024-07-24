package yds

import (
	"crypto/tls"
	"fmt"
	"path"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/log/corelogadapter"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/tross/library/go/core/log"
	"github.com/doublecloud/tross/library/go/core/metrics"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/config/env"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/logbroker"
	ydbcommon "github.com/doublecloud/tross/transfer_manager/go/pkg/providers/ydb"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/xtls"
)

func MakeWriterConfigFactory(tlsConfig *tls.Config, serviceAccountID string, credentials ydb.Credentials) logbroker.WriterConfigFactory {
	return func(config *logbroker.LbDestination, shard, groupID, topic string, extras map[string]string, logger log.Logger) (*persqueue.WriterOptions, error) {
		sourceID := fmt.Sprintf("%v_%v", shard, groupID)
		fullTopicName := path.Join(config.Database, topic)

		opts := persqueue.WriterOptions{
			Credentials:    credentials,
			Endpoint:       config.Instance,
			Database:       config.Database,
			Topic:          fullTopicName,
			SourceID:       []byte(sourceID),
			Logger:         corelogadapter.New(logger),
			RetryOnFailure: true,
			Codec:          persqueue.Raw,
			ExtraAttrs:     extras,
			Port:           config.Port,
			TLSConfig:      tlsConfig,
		}

		logger.Info("try to init persqueue writer",
			log.String("Endpoint", opts.Endpoint),
			log.Int("Port", opts.Port),
			log.String("Database", opts.Database),
			log.String("Topic", fullTopicName),
			log.String("SourceID", sourceID),
		)

		return &opts, nil
	}
}

func NewSink(cfg *YDSDestination, registry metrics.Registry, lgr log.Logger, transferID string) (abstract.Sinker, error) {
	writerFactory := logbroker.DefaultWriterFactory
	useTopicAPI := false

	if env.Get() == env.EnvironmentNebius {
		writerFactory = logbroker.NewYDSWriterFactory
		useTopicAPI = true
	}

	var tlsConfig *tls.Config
	if cfg.TLSEnalbed {
		var err error
		tlsConfig, err = xtls.FromPath(cfg.RootCAFiles)
		if err != nil {
			return nil, xerrors.Errorf("failed to obtain TLS configuration for cloud: %w", err)
		}
	}

	creds, err := ydbcommon.ResolveCredentials(
		cfg.UserdataAuth,
		string(cfg.Token),
		ydbcommon.JWTAuthParams{
			KeyContent:      cfg.SAKeyContent,
			TokenServiceURL: cfg.TokenServiceURL,
		},
		cfg.ServiceAccountID,
		logger.Log,
	)
	if err != nil {
		return nil, xerrors.Errorf("Cannot create YDB credentials: %w", err)
	}

	return logbroker.NewSinkWithFactory(
		&cfg.LbDstConfig,
		registry,
		lgr,
		transferID,
		MakeWriterConfigFactory(tlsConfig, cfg.ServiceAccountID, creds),
		writerFactory,
		useTopicAPI,
		false,
	)
}
