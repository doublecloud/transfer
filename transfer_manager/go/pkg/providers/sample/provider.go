package sample

import (
	"encoding/gob"

	"github.com/doublecloud/tross/library/go/core/log"
	"github.com/doublecloud/tross/library/go/core/metrics"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers"
)

func init() {
	gob.Register(new(SampleSource))
	server.RegisterSource(ProviderType, func() server.Source {
		return new(SampleSource)
	})
	abstract.RegisterProviderName(ProviderType, "Sample")
	providers.Register(ProviderType, func(lgr log.Logger, registry metrics.Registry, cp coordinator.Coordinator, transfer *server.Transfer) providers.Provider {
		return &Provider{
			logger:   lgr,
			registry: registry,
			cp:       cp,
			transfer: transfer,
		}
	})
}

const ProviderType = abstract.ProviderType("sample")

var (
	_ providers.Replication = (*Provider)(nil)
)

type Provider struct {
	logger   log.Logger
	registry metrics.Registry
	cp       coordinator.Coordinator
	transfer *server.Transfer
}

func (p Provider) Type() abstract.ProviderType {
	return ProviderType
}

func (p Provider) Source() (abstract.Source, error) {
	src, ok := p.transfer.Src.(*SampleSource)
	if !ok {
		return nil, xerrors.Errorf("unexpected source type: %T", p.transfer.Src)
	}

	return NewSource(src, p.transfer.ID, p.logger, p.registry)
}
