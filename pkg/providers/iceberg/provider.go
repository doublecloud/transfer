package iceberg

import (
	"encoding/gob"

	"github.com/doublecloud/transfer/library/go/core/metrics"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/model"
	"go.ytsaurus.tech/library/go/core/log"
)

const ProviderType = abstract.ProviderType("iceberg")

func init() {
	sourceFactory := func() model.Source {
		return new(IcebergSource)
	}

	gob.Register(new(IcebergSource))
	model.RegisterSource(ProviderType, sourceFactory)
	abstract.RegisterProviderName(ProviderType, "Delta Lake")
}

type Provider struct {
	logger   log.Logger
	registry metrics.Registry
	transfer *model.Transfer
}

func (p Provider) Type() abstract.ProviderType {
	return ProviderType
}

func (p Provider) Storage() (abstract.Storage, error) {
	src, ok := p.transfer.Src.(*IcebergSource)
	if !ok {
		return nil, xerrors.Errorf("unexpected src type: %T", p.transfer.Src)
	}

	return NewStorage(src, p.logger, p.registry)
}
