package logbroker

import (
	"github.com/doublecloud/tross/library/go/core/metrics"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewSource(cfg *LfSource, logger log.Logger, registry metrics.Registry) (abstract.Source, error) {
	return NewSourceWithRetries(cfg, logger, registry, 100)
}

func NewSourceWithRetries(cfg *LfSource, logger log.Logger, registry metrics.Registry, retries int) (abstract.Source, error) {
	if cfg.Cluster != "" && len(KnownClusters[cfg.Cluster]) > 0 {
		result, err := NewMultiDCSource(cfg, logger, registry)
		if err != nil {
			return nil, xerrors.Errorf("unable to create multi-dc source, err: %w", err)
		}
		return result, nil
	}
	result, err := NewOneDCSource(cfg, logger, stats.NewSourceStats(registry), retries)
	if err != nil {
		return nil, xerrors.Errorf("unable to create one-dc source, err: %w", err)
	}
	return result, nil
}
