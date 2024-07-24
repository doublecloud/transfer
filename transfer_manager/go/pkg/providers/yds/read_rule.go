package yds

import (
	"context"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/controlplane"
	Ydb_Persqueue_Protos_V1 "github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/genproto/Ydb_PersQueue_V1"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/log/corelogadapter"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/session"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	ydbcommon "github.com/doublecloud/tross/transfer_manager/go/pkg/providers/ydb"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/util"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/xtls"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func convertCodecs(src []YdsCompressionCodec) (dst []Ydb_Persqueue_Protos_V1.Codec) {
	dst = make([]Ydb_Persqueue_Protos_V1.Codec, 0, len(src))
	for _, srcItem := range src {
		dst = append(dst, Ydb_Persqueue_Protos_V1.Codec(srcItem))
	}
	return dst
}

func newPQOptions(src *YDSSource) (*session.Options, error) {
	opts := &session.Options{
		Endpoint: src.Endpoint,
		Port:     src.Port,
		Logger:   corelogadapter.New(logger.Log),
		Database: src.Database,
	}
	if src.TLSEnalbed {
		tls, err := xtls.FromPath(src.RootCAFiles)
		if err != nil {
			return nil, xerrors.Errorf("error getting TLS config: %w", err)
		}
		opts.TLSConfig = tls
	}
	opts.Credentials = src.Credentials
	if opts.Credentials == nil {
		var err error
		opts.Credentials, err = ydbcommon.ResolveCredentials(
			src.UserdataAuth,
			string(src.Token),
			ydbcommon.JWTAuthParams{
				KeyContent:      src.SAKeyContent,
				TokenServiceURL: src.TokenServiceURL,
			},
			src.ServiceAccountID,
			logger.Log,
		)
		if err != nil {
			return nil, xerrors.Errorf("Cannot create YDB credentials: %w", err)
		}
	}
	return opts, nil
}

func CreateReadRule(src *YDSSource, defaultConsumer string) error {
	consumer := src.Consumer
	if consumer == "" {
		consumer = defaultConsumer
	}
	return backoff.RetryNotify(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
		defer cancel()
		opts, err := newPQOptions(src)
		if err != nil {
			return xerrors.Errorf("unable to init pq options: %w", err)
		}
		lbControlPlane, err := controlplane.NewControlPlaneClient(ctx, *opts)
		if err != nil {
			return xerrors.Errorf("unable to init pq cp client: %w", err)
		}
		defer lbControlPlane.Close()
		dRes, err := lbControlPlane.DescribeTopic(ctx, src.Stream)
		if err != nil {
			return xerrors.Errorf("unable to describe topic: %s, err: %w", src.Stream, err)
		}
		logger.Log.Infof("topic info: %v", dRes)
		if readRuleExists(dRes, consumer) {
			return nil
		}
		if err = lbControlPlane.AddReadRule(ctx, &Ydb_Persqueue_Protos_V1.AddReadRuleRequest{
			Path: src.Stream,
			ReadRule: &Ydb_Persqueue_Protos_V1.TopicSettings_ReadRule{
				ConsumerName:    consumer,
				Important:       false,
				SupportedFormat: Ydb_Persqueue_Protos_V1.TopicSettings_FORMAT_BASE,
				SupportedCodecs: convertCodecs(src.GetSupportedCodecs()),
				Version:         1,
				// ServiceType:     "data-transfer",
			},
		}); err != nil {
			if strings.Contains(err.Error(), "ALREADY_EXISTS") {
				return nil
			}
			return err
		}
		return nil
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3), util.BackoffLogger(logger.Log, "add read-rule"))
}

func readRuleExists(topicDescription *Ydb_Persqueue_Protos_V1.DescribeTopicResult, consumer string) bool {
	for _, rule := range topicDescription.Settings.ReadRules {
		if rule.ConsumerName == consumer {
			logger.Log.Infof("Read rule for consumer %s already exists: important = %v, starting message timestamp(ms) = %v", consumer, rule.Important, rule.StartingMessageTimestampMs)
			return true
		}
	}
	return false
}

func DropReadRule(src *YDSSource, defaultConsumer string) error {
	if err := backoff.Retry(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
		defer cancel()
		opts, err := newPQOptions(src)
		if err != nil {
			return xerrors.Errorf("unable to get new persqueue options from YDS source: %w", err)
		}
		lbControlPlane, err := controlplane.NewControlPlaneClient(ctx, *opts)
		if err != nil {
			return xerrors.Errorf("unable to get new persqueue controlplane client: %w", err)
		}
		defer lbControlPlane.Close()
		dRes, err := lbControlPlane.DescribeTopic(ctx, src.Stream)
		if err != nil {
			return xerrors.Errorf("unable to get describe topic result from persqueue controlplane: %w", err)
		}
		logger.Log.Infof("topic info: %v", dRes)
		consumer := src.Consumer
		if consumer == "" {
			consumer = defaultConsumer
		}
		if err := lbControlPlane.RemoveReadRule(ctx, &Ydb_Persqueue_Protos_V1.RemoveReadRuleRequest{
			Path:         src.Stream,
			ConsumerName: consumer,
		}); err != nil {
			if errStatus, ok := status.FromError(err); ok && errStatus.Code() == codes.NotFound {
				logger.Log.Infof("unable to remove read rule (path=%s, consumer=%s): %v", src.Stream, consumer, err)
			} else {
				return xerrors.Errorf("unable to remove read rule from persqueue controlplane: %w", err)
			}
		}
		return nil
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 10)); err != nil {
		return err
	}
	return nil
}
