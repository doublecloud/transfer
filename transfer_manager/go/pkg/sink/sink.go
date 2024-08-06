package sink

import (
	"context"
	"time"

	"github.com/doublecloud/tross/library/go/core/metrics"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/config/env"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/errors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/errors/categories"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/middlewares"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/middlewares/async"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/middlewares/async/bufferer"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/middlewares/memthrottle"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/stats"
	"github.com/dustin/go-humanize"
	"go.ytsaurus.tech/library/go/core/log"
)

// NoAsyncSinkErr error which indicate that we should try to create sinker from SyncSink
var NoAsyncSinkErr = xerrors.NewSentinel("no applicable AsyncSink for this transfer")

// MakeAsyncSink creates a ready-to-use complete sink pipeline, topped with an asynchronous sink wrapper.
// The pipeline may include multiple middlewares and transformations. Their concrete set depends on transfer settings, its source and destination.
func MakeAsyncSink(transfer *server.Transfer, lgr log.Logger, mtrcs metrics.Registry, cp coordinator.Coordinator, config middlewares.Config, opts ...abstract.SinkOption) (abstract.AsyncSink, error) {
	var pipelineAsync abstract.AsyncSink = nil
	middleware, err := syncMiddleware(transfer, lgr, mtrcs, cp, opts...)
	if err != nil {
		return nil, xerrors.Errorf("error building sync middleware pipeline: %w", err)
	}

	pipelineAsync, err = constructBaseAsyncSink(transfer, lgr, mtrcs, cp, middleware)
	if err != nil {
		if !xerrors.Is(err, NoAsyncSinkErr) {
			return nil, errors.CategorizedErrorf(categories.Target, "failed to construct async sink: %w", err)
		}
		sink, err := constructBaseSink(transfer, lgr, mtrcs, cp, config)
		if err != nil {
			return nil, errors.CategorizedErrorf(categories.Target, "failed to construct sink: %w", err)
		}
		pipelineAsync = wrapSinkIntoAsyncPipeline(sink, transfer, lgr, mtrcs, middleware, config)
	}

	pipelineAsync = async.Measurer(lgr)(pipelineAsync)
	return pipelineAsync, nil
}

func syncMiddleware(transfer *server.Transfer, lgr log.Logger, mtrcs metrics.Registry, cp coordinator.Coordinator, opts ...abstract.SinkOption) (abstract.Middleware, error) {
	transformer, err := middlewares.Transformation(transfer, lgr, mtrcs)
	if err != nil {
		return nil, xerrors.Errorf("unable to set transformation middleware: %w", err)
	}
	return func(pipeline abstract.Sinker) abstract.Sinker {
		fallbackStats := stats.NewFallbackStatsCombination(mtrcs)
		pipeline = middlewares.TargetFallbacks(transfer.TypeSystemVersion, transfer.DstType(), lgr, fallbackStats.Target)(pipeline)
		pipeline = middlewares.SourceFallbacks(transfer.TypeSystemVersion, transfer.SrcType(), lgr, fallbackStats.Source)(pipeline)

		pipeline = middlewares.OutputDataMetering()(pipeline)

		pipeline = middlewares.Statistician(lgr, stats.NewWrapperStats(mtrcs))(pipeline)
		if dst, ok := transfer.Dst.(server.SystemTablesDependantDestination); !ok || !dst.ReliesOnSystemTablesTransferring() {
			pipeline = middlewares.Filter(mtrcs, middlewares.ExcludeSystemTables)(pipeline)
		}

		// TODO: apply this middleware for selected sinkers only
		pipeline = middlewares.NonRowSeparator()(pipeline)

		if transfer.Src.GetProviderType() != transfer.Dst.GetProviderType() && env.IsTest() {
			// only check type strictness in heterogenous transfers
			pipeline = middlewares.TypeStrictnessTracker(lgr, stats.NewTypeStrictnessStats(mtrcs))(pipeline)
		}

		pipeline = middlewares.PluggableTransformersChain(transfer, mtrcs, cp)(pipeline)
		pipeline = transformer(pipeline)

		for i := range opts {
			pipeline = opts[i](pipeline)
		}

		pipeline = middlewares.InputDataMetering()(pipeline)
		return pipeline
	}, nil
}

// constructBaseSink creates a sink of proper type
func constructBaseSink(transfer *server.Transfer, lgr log.Logger, mtrcs metrics.Registry, cp coordinator.Coordinator, config middlewares.Config) (abstract.Sinker, error) {
	switch dst := transfer.Dst.(type) {
	case *server.MockDestination:
		return dst.SinkerFactory(), nil
	default:
		if !config.ReplicationStage {
			factory, ok := providers.Destination[providers.SnapshotSinker](lgr, mtrcs, cp, transfer)
			if ok {
				res, err := factory.SnapshotSink(config)
				if err != nil {
					return nil, xerrors.Errorf("unable to create %T: %w", transfer.Src, err)
				}
				return res, nil
			}
		}
		factory, ok := providers.Destination[providers.Sinker](lgr, mtrcs, cp, transfer)
		if !ok {
			return nil, xerrors.Errorf("sink: %s: %T not supported", transfer.DstType(), transfer.Dst)
		}
		res, err := factory.Sink(config)
		if err != nil {
			return nil, xerrors.Errorf("unable to create %T: %w", transfer.Dst, err)
		}
		return res, nil
	}
}

func constructBaseAsyncSink(transfer *server.Transfer, lgr log.Logger, mtrcs metrics.Registry, cp coordinator.Coordinator, middleware abstract.Middleware) (abstract.AsyncSink, error) {
	if asyncF, ok := providers.Destination[providers.AsyncSinker](lgr, mtrcs, cp, transfer); ok {
		return asyncF.AsyncSink(middleware)
	}
	return nil, NoAsyncSinkErr
}

func wrapSinkIntoAsyncPipeline(sink abstract.Sinker, transfer *server.Transfer, lgr log.Logger, mtrcs metrics.Registry, middleware abstract.Middleware, config middlewares.Config) abstract.AsyncSink {
	sink = middlewares.ErrorTracker(mtrcs)(sink)
	if config.EnableRetries {
		sink = middlewares.Retrier(lgr, context.Background())(sink)
	}
	sink = middleware(sink)

	var pipelineAsync abstract.AsyncSink
	if bConfig := calculateBuffererConfig(transfer, config, lgr); bConfig != nil {
		pipelineAsync = bufferer.Bufferer(lgr, *bConfig, mtrcs)(sink)
		if memLimit, isEnabled := getMemoryThrottlerSettings(transfer); isEnabled {
			lgr.Info("memory throttler is enabled", log.String("limit", humanize.Bytes(memLimit)))
			pipelineAsync = memthrottle.MemoryThrottler(memthrottle.DefaultConfig(memLimit), lgr)(pipelineAsync)
		}
	} else {
		pipelineAsync = async.Synchronizer(lgr)(sink)
	}
	return pipelineAsync
}

func calculateBuffererConfig(transfer *server.Transfer, middlewaresConfig middlewares.Config, lgr log.Logger) *bufferer.BuffererConfig {
	if middlewaresConfig.NoData {
		return nil
	}

	bufferableDst, ok := transfer.Dst.(bufferer.Bufferable)
	if !ok {
		return nil
	}

	result := bufferableDst.BuffererConfig()

	if middlewaresConfig.ReplicationStage {
		if result.TriggingInterval == 0 {
			result.TriggingInterval = 333 * time.Millisecond
		}
	}

	// XXX: drop when https://st.yandex-team.ru/TM-4545 is resolved
	if result.TriggingInterval == 0 {
		result.TriggingInterval = 1 * time.Second
	}

	lgr.Info("bufferer config was calculated", log.Int("trigging_count", result.TriggingCount),
		log.UInt64("trigging_size", result.TriggingSize), log.Float64("trigging_interval_seconds", result.TriggingInterval.Seconds()))
	return &result
}

func getMemoryThrottlerSettings(transfer *server.Transfer) (uint64, bool) {
	if val, err := transfer.SystemLabel(server.SystemLabelMemThrottle); err == nil && val == "on" {
		if rt, ok := transfer.Runtime.(abstract.LimitedResourceRuntime); ok {
			return rt.RAMGuarantee(), rt.RAMGuarantee() != 0
		}
	}
	return 0, false
}
