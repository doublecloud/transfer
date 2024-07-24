package storage

import (
	"github.com/doublecloud/tross/library/go/core/metrics"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers"
)

var UnsupportedSourceErr = xerrors.New("Unsupported storage")

func NewStorage(transfer *server.Transfer, cp coordinator.Coordinator, registry metrics.Registry) (abstract.Storage, error) {
	switch src := transfer.Src.(type) {
	case *server.MockSource:
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
