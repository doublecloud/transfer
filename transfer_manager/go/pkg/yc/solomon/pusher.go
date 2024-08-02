package ycsolomon

import (
	"context"
	"encoding/json"
	sync "sync"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/tross/library/go/core/log"
	"github.com/doublecloud/tross/library/go/core/metrics/solomon"
	"github.com/doublecloud/tross/library/go/yandex/solomon/reporters/pusher/httppusher"
	"github.com/doublecloud/tross/transfer_manager/go/internal/metrics"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/util"
	yccreds "github.com/doublecloud/tross/transfer_manager/go/pkg/yc/credentials"
)

func RunPusher(ctx context.Context, solomonPusher *httppusher.Pusher, metrics *solomon.Registry, wg *sync.WaitGroup, logger log.Logger) {
	defer wg.Done()
	creds, err := yccreds.NewIamCreds(logger)
	if err != nil {
		logger.Errorf("cannot init iam token client: %v", err)
	}
	tick := time.NewTicker(15 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			<-tick.C
			Push(ctx, creds, solomonPusher, metrics, logger)
			return
		case <-tick.C:
			Push(ctx, creds, solomonPusher, metrics, logger)
		}
	}
}

func Push(ctx context.Context, creds ydb.Credentials, solomonPusher *httppusher.Pusher, metrics *solomon.Registry, logger log.Logger) {
	sensors, err := metrics.Gather()
	if err != nil {
		logger.Error("cannot gather metrics from registry", log.Error(err))
		return
	}
	token, err := creds.Token(context.TODO())
	if err != nil {
		logger.Error("unable to get iam token to push metrics", log.Error(err))
		return
	}
	setIamToken := httppusher.WithIAMToken(token)
	if err := setIamToken(solomonPusher); err != nil {
		logger.Error("cannot set iam tokent", log.Error(err))
	}
	if err := solomonPusher.Push(ctx, sensors); err != nil {
		serializedSensors, serializeErr := json.Marshal(sensors)
		if serializeErr != nil {
			logger.Warn("cannot serialize sensors", log.Error(serializeErr))
			return
		}
		logger.Error("cannot push metrics", log.Error(err), log.String("sensors", util.Sample(string(serializedSensors), 1000)))
		return
	}
}

func NewHTTPPusher(cloudID, folderID, service, host string, logger log.Logger) (*httppusher.Pusher, error) {
	popts := []httppusher.PusherOpt{
		httppusher.SetService(service),
		httppusher.SetCluster(folderID),
		httppusher.SetProject(cloudID),
		httppusher.WithLogger(logger.Structured()),
		httppusher.WithHTTPHost(host),
		httppusher.WithMetricsChunkSize(metrics.SolomonMetricsChunkSize),
	}
	pusher, err := httppusher.NewPusher(popts...)
	if err != nil {
		logger.Fatal("failed to create monitoring metrics pusher", log.Error(err))
		return nil, err
	}
	return pusher, nil
}

type SolomonPusher struct {
	pusher  *httppusher.Pusher
	creds   ydb.Credentials
	metrics *solomon.Registry
	logger  log.Logger
}

func (p *SolomonPusher) Push(ctx context.Context) {
	Push(ctx, p.creds, p.pusher, p.metrics, p.logger)
}

func (p *SolomonPusher) Run(ctx context.Context) {
	tick := time.NewTicker(15 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			p.Push(ctx)
		}
	}
}

func NewSolomonPusher(pusher *httppusher.Pusher, metrics *solomon.Registry, logger log.Logger) (*SolomonPusher, error) {
	creds, err := yccreds.NewIamCreds(logger)
	if err != nil {
		return nil, err
	}
	return &SolomonPusher{
		pusher:  pusher,
		metrics: metrics,
		logger:  logger,
		creds:   creds,
	}, nil
}
