package stdout

import (
	"encoding/gob"

	"github.com/doublecloud/transfer/library/go/core/metrics"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/coordinator"
	"github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/middlewares"
	"github.com/doublecloud/transfer/pkg/providers"
	"go.ytsaurus.tech/library/go/core/log"
)

func init() {
	gob.RegisterName("*server.StdoutDestination", new(StdoutDestination))
	gob.RegisterName("*server.EmptySource", new(EmptySource))
	model.RegisterSource(ProviderType, sourceModelFactory)
	model.RegisterDestination(ProviderType, destinationModelFactory)
	model.RegisterDestination(ProviderTypeStdout, destinationModelFactory)
	abstract.RegisterProviderName(ProviderType, "Empty")
	abstract.RegisterProviderName(ProviderTypeStdout, "Stdout")
	providers.Register(ProviderType, New(ProviderType))
	providers.Register(ProviderTypeStdout, New(ProviderTypeStdout))
}

func destinationModelFactory() model.Destination {
	return new(StdoutDestination)
}

func sourceModelFactory() model.Source {
	return new(EmptySource)
}

const ProviderTypeStdout = abstract.ProviderType("stdout")
const ProviderType = abstract.ProviderType("empty")

// To verify providers contract implementation
var (
	_ providers.Sinker = (*Provider)(nil)
)

type Provider struct {
	logger   log.Logger
	registry metrics.Registry
	cp       coordinator.Coordinator
	transfer *model.Transfer
	provider abstract.ProviderType
}

func (p *Provider) Type() abstract.ProviderType {
	return p.provider
}

func (p *Provider) Sink(middlewares.Config) (abstract.Sinker, error) {
	dst, ok := p.transfer.Dst.(*StdoutDestination)
	if !ok {
		return nil, xerrors.Errorf("unexpected target type: %T", p.transfer.Dst)
	}
	return NewSinker(p.logger, dst, p.registry), nil
}

func New(provider abstract.ProviderType) func(lgr log.Logger, registry metrics.Registry, cp coordinator.Coordinator, transfer *model.Transfer) providers.Provider {
	return func(lgr log.Logger, registry metrics.Registry, cp coordinator.Coordinator, transfer *model.Transfer) providers.Provider {
		return &Provider{
			logger:   lgr,
			registry: registry,
			cp:       cp,
			transfer: transfer,
			provider: provider,
		}
	}
}
