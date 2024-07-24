package fedlog

import (
	"time"

	"github.com/doublecloud/tross/library/go/yandex/ydb/ydb-topic-fed-sdk/fedtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
)

func FedDriver(logger log.Logger, fedDetails fedtrace.Detailer) (res fedtrace.FedDriver) {
	res.OnConnect = func(startInfo fedtrace.FedDriverClusterConnectionStartInfo) func(fedtrace.FedDriverClusterConnectionDoneInfo) {
		if fedDetails.Details()&fedtrace.ClusterConnection == 0 {
			return nil
		}

		start := time.Now()
		ctx := *startInfo.Context
		logLevel(ctx, log.DEBUG, logger, "Start connection to the cluster", log.Stringer("db_info", &startInfo.DBInfo), versionField())
		return func(info fedtrace.FedDriverClusterConnectionDoneInfo) {
			if info.Error == nil {
				logLevel(ctx, log.INFO, logger, "Connected to the cluster",
					log.Any("db_info", startInfo.DBInfo),
					latencyField(start),
					versionField())
			} else {
				logLevel(ctx, log.INFO, logger, "Connection to the cluster failed",
					log.Any("db_info", startInfo.DBInfo),
					latencyField(start),
					log.Error(info.Error),
					versionField(),
				)
			}
		}
	}

	res.OnDiscovery = func(info fedtrace.FedDriverDiscoveryStartInfo) func(fedtrace.FedDriverDiscoveryDoneInfo) {
		if fedDetails.Details()&fedtrace.Discovery == 0 {
			return nil
		}

		start := time.Now()
		ctx := *info.Context
		logLevel(ctx, log.DEBUG, logger, "Federated discovery started", log.Any("oldDbInfo", info.OldDBInfo))
		return func(info fedtrace.FedDriverDiscoveryDoneInfo) {
			fields := []log.Field{latencyField(start), versionField(), log.Any("db_info", info.DBInfo)}
			if info.Error == nil {
				logLevel(ctx, log.INFO, logger, "Federated discovery finished", fields...)
			} else {
				logLevel(ctx, log.ERROR, logger, "Federated query failed", append(fields, log.Error(info.Error))...)
			}
		}
	}

	res.OnDiscoveryStop = func(info fedtrace.FedDriverDiscoveryStopInfo) {
		if fedDetails.Details()&fedtrace.Discovery == 0 {
			return
		}
		ctx := *info.Context
		logLevel(ctx, log.INFO, logger, "Federated discovery stopped",
			log.Any("last_db_info", info.LastDBInfo),
			log.String("reason", info.Reason.Error()),
		)
	}

	return res
}
