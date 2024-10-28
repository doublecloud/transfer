package storage

import (
	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/metrics"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/coordinator"
	"github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/providers"
)

var UnsupportedSourceErr = xerrors.New("Unsupported storage")

func NewStorage(transfer *model.Transfer, cp coordinator.Coordinator, registry metrics.Registry) (abstract.Storage, error) {
	switch src := transfer.Src.(type) {
	case *model.MockSource:
		return src.StorageFactory(), nil
	default:
		snapshoter, ok := providers.Source[providers.Snapshot](logger.Log, registry, cp, transfer)
		if !ok {
			return nil, xerrors.Errorf("%w: %s: %T", UnsupportedSourceErr, transfer.SrcType(), transfer.Src)
		}
		res, err := snapshoter.Storage()
		if err != nil {
			return nil, xerrors.Errorf("unable to create %T: %w", transfer.Src, err)
		}
		return res, nil
	}
}
