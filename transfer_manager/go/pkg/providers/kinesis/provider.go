package kinesis

import (
	"context"
	"encoding/gob"

	"github.com/doublecloud/transfer/library/go/core/metrics"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	cpclient "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers"
	"go.ytsaurus.tech/library/go/core/log"
)

func init() {
	gob.Register(new(KinesisSource))
	server.RegisterSource(ProviderType, func() server.Source {
		return new(KinesisSource)
	})
	abstract.RegisterProviderName(ProviderType, "Kinesis")

	providers.Register(ProviderType, New)
}

const ProviderType = abstract.ProviderType("kinesis")

var (
	_ providers.Replication = (*Provider)(nil)

	_ providers.Activator = (*Provider)(nil)
)

type Provider struct {
	logger   log.Logger
	registry metrics.Registry
	cp       cpclient.Coordinator
	transfer *server.Transfer
}

func (p *Provider) Type() abstract.ProviderType {
	return ProviderType
}

func (p *Provider) Source() (abstract.Source, error) {
	src, ok := p.transfer.Src.(*KinesisSource)
	if !ok {
		return nil, xerrors.Errorf("unexpected source type: %T", p.transfer.Src)
	}
	return NewSource(p.transfer.ID, p.cp, src, p.logger, p.registry)
}

func (p *Provider) Activate(context.Context, *server.TransferOperation, abstract.TableMap, providers.ActivateCallbacks) error {
	if p.transfer.SrcType() == ProviderType && !p.transfer.IncrementOnly() {
		return xerrors.New("Only allowed mode for kinesis source is replication")
	}
	return nil
}

func New(lgr log.Logger, registry metrics.Registry, cp cpclient.Coordinator, transfer *server.Transfer) providers.Provider {
	return &Provider{
		logger:   lgr,
		registry: registry,
		cp:       cp,
		transfer: transfer,
	}
}
