package dbt

import (
	"context"
	"time"

	"github.com/doublecloud/tross/library/go/core/metrics"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/errors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/errors/categories"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/transformer"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

func PluggableTransformer(transfer *server.Transfer, _ metrics.Registry, cp coordinator.Coordinator) func(abstract.Sinker) abstract.Sinker {
	supportedDestination, err := ToSupportedDestination(transfer.Dst)
	if err != nil {
		return IdentityMiddleware
	}

	if transfer.Transformation == nil || transfer.Transformation.Transformers == nil {
		return IdentityMiddleware
	}
	dbtConfigurations, _ := dbConfigs(transfer.Transformation.Transformers)
	if dbtConfigurations == nil {
		return IdentityMiddleware
	}

	return func(s abstract.Sinker) abstract.Sinker {
		return newPluggableTransformer(s, cp, transfer, context.Background(), supportedDestination, dbtConfigurations)
	}
}

var IdentityMiddleware = func(s abstract.Sinker) abstract.Sinker { return s }

func dbConfigs(transformers *transformer.Transformers) ([]*Config, error) {
	result := make([]*Config, 0)
	for _, t := range transformers.Transformers {
		if v, ok := t[TransformerType]; ok {
			var cfg Config
			if err := util.MapFromJSON(v, &cfg); err != nil {
				return nil, xerrors.Errorf("unable to map %T to %T: %w", v, cfg, err)
			}
			result = append(result, &cfg)
		}
	}
	return result, nil
}

type pluggableTransformer struct {
	executedByMainWorker bool

	dst            SupportedDestination
	configurations []*Config

	sink     abstract.Sinker
	cp       coordinator.Coordinator
	transfer *server.Transfer
}

func newPluggableTransformer(
	s abstract.Sinker,
	cp coordinator.Coordinator,
	transfer *server.Transfer,
	ctx context.Context,
	dst SupportedDestination,
	configurations []*Config,
) *pluggableTransformer {
	return &pluggableTransformer{
		executedByMainWorker: false,

		dst:            dst,
		configurations: configurations,

		sink:     s,
		cp:       cp,
		transfer: transfer,
	}
}

func (r *pluggableTransformer) Close() error {
	sinkCloseResult := r.sink.Close()
	if sinkCloseResult != nil {
		return sinkCloseResult
	}
	if !r.executedByMainWorker {
		return nil
	}

	logger.Log.Info("running DBT transformation(s)", log.Int("dbt_transformations_count", len(r.configurations)))
	dbtStartT := time.Now()
	if err := r.run(); err != nil {
		return xerrors.Errorf("DBT transformation(s) failed: %w", err)
	}
	logger.Log.Info("DBT transformation(s) executed successfully", log.Duration("elapsed", time.Since(dbtStartT)), log.Int("dbt_transformations_count", len(r.configurations)))
	return nil
}

func (r *pluggableTransformer) run() error {
	ctx := context.Background()
	for configurationI, configuration := range r.configurations {
		if err := newRunner(r.dst, configuration, r.transfer).Run(ctx); err != nil {
			if errOSM := r.cp.OpenStatusMessage(
				r.transfer.ID,
				"dbt",
				errors.ToTransferStatusMessage(errors.CategorizedErrorf(categories.Target, "failed to run DBT transformation [%d] in the target database: %w", configurationI, err)),
			); errOSM != nil {
				logger.Log.Warn("failed to open a status message for a DBT error", log.Error(errOSM), log.NamedError("dbt_error", err))
			}
			logger.Log.Error("DBT transformation failed", log.Int("transformation_i", configurationI), log.Error(err))
			return errors.CategorizedErrorf(categories.Target, "failed to run DBT transformation [%d] in the target database: %w", configurationI, err)
		}
	}
	return nil
}

func (r *pluggableTransformer) Push(input []abstract.ChangeItem) error {
	if !r.executedByMainWorker {
		if abstract.FindItemOfKind(input, abstract.DoneShardedTableLoad) != nil {
			r.executedByMainWorker = true
		}
	}
	return r.sink.Push(input)
}
