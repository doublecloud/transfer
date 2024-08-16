package stdout

import (
	"encoding/gob"

	"github.com/doublecloud/transfer/library/go/core/metrics"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/middlewares"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers"
	"go.ytsaurus.tech/library/go/core/log"
)

func init() {
	gob.RegisterName("*server.StdoutDestination", new(StdoutDestination))
	gob.RegisterName("*server.EmptySource", new(EmptySource))
	server.RegisterSource(ProviderType, sourceModelFactory)
	server.RegisterDestination(ProviderType, destinationModelFactory)
	server.RegisterDestination(ProviderTypeStdout, destinationModelFactory)
	abstract.RegisterProviderName(ProviderType, "Empty")
	abstract.RegisterProviderName(ProviderTypeStdout, "Stdout")
	providers.Register(ProviderType, New(ProviderType))
	providers.Register(ProviderTypeStdout, New(ProviderTypeStdout))
}

func destinationModelFactory() server.Destination {
	return new(StdoutDestination)
}

func sourceModelFactory() server.Source {
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
	transfer *server.Transfer
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

func New(provider abstract.ProviderType) func(lgr log.Logger, registry metrics.Registry, cp coordinator.Coordinator, transfer *server.Transfer) providers.Provider {
	return func(lgr log.Logger, registry metrics.Registry, cp coordinator.Coordinator, transfer *server.Transfer) providers.Provider {
		return &Provider{
			logger:   lgr,
			registry: registry,
			cp:       cp,
			transfer: transfer,
			provider: provider,
		}
	}
}
