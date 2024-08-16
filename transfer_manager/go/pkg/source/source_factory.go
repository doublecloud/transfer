package source

import (
	"github.com/doublecloud/transfer/library/go/core/metrics"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewSource(transfer *server.Transfer, lgr log.Logger, registry metrics.Registry, cp coordinator.Coordinator) (abstract.Source, error) {
	replicator, ok := providers.Source[providers.Replication](lgr, registry, cp, transfer)
	if !ok {
		lgr.Error("Unable to create source")
		return nil, xerrors.Errorf("unknown source: %s: %T", transfer.SrcType(), transfer.Src)
	}
	res, err := replicator.Source()
	if err != nil {
		return nil, xerrors.Errorf("unable to create %T: %w", transfer.Src, err)
	}
	return res, nil
}
