package targets

import (
	"github.com/doublecloud/transfer/library/go/core/metrics"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/base"
	"github.com/doublecloud/transfer/pkg/providers"
	"go.ytsaurus.tech/library/go/core/log"
)

var UnknownTargetError = xerrors.New("unknown event target for destination, try legacy sinker instead")

func NewTarget(transfer *server.Transfer, lgr log.Logger, mtrcs metrics.Registry, cp coordinator.Coordinator, opts ...abstract.SinkOption) (t base.EventTarget, err error) {
	if factory, ok := providers.Destination[providers.Abstract2Sinker](lgr, mtrcs, cp, transfer); ok {
		return factory.Target(opts...)
	}
	return nil, UnknownTargetError
}
