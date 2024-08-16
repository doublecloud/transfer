package data

import (
	"github.com/doublecloud/transfer/library/go/core/metrics"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/base"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers"
	"go.ytsaurus.tech/library/go/core/log"
)

var (
	TryLegacySourceError = xerrors.New("Not found in new sources, try legacy source")
)

func NewDataProvider(lgr log.Logger, registry metrics.Registry, transfer *server.Transfer, cp coordinator.Coordinator) (p base.DataProvider, err error) {
	dataer, ok := providers.Source[providers.Abstract2Provider](lgr, registry, cp, transfer)
	if !ok {
		return nil, TryLegacySourceError
	}
	res, err := dataer.DataProvider()
	if err == TryLegacySourceError {
		return nil, TryLegacySourceError
	}
	if err != nil {
		return nil, xerrors.Errorf("unable to create %T: %w", transfer.Src, err)
	}
	return res, nil
}

func NewSnapshotProvider(lgr log.Logger, registry metrics.Registry, transfer *server.Transfer, cp coordinator.Coordinator) (base.SnapshotProvider, error) {
	p, err := NewDataProvider(lgr, registry, transfer, cp)
	if err != nil {
		return nil, xerrors.Errorf("unable to get data provider: %w", err)
	}

	sp, ok := p.(base.SnapshotProvider)
	if !ok {
		return nil, xerrors.Errorf("data provider for this source does not support snapshot")
	}
	return sp, nil
}
