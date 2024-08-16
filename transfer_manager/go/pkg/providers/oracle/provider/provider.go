package provider

import (
	"github.com/doublecloud/transfer/library/go/core/metrics"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	cpclient "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/base"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/oracle"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/s3"
	"go.ytsaurus.tech/library/go/core/log"
)

func init() {
	providers.Register(oracle.ProviderType, New)
}

// To verify providers contract implementation
var (
	_ providers.Abstract2Provider = (*Provider)(nil)
)

type Provider struct {
	logger   log.Logger
	registry metrics.Registry
	cp       cpclient.Coordinator
	transfer *server.Transfer
}

func (p *Provider) DataProvider() (base.DataProvider, error) {
	specificConfig, ok := p.transfer.Src.(*oracle.OracleSource)
	if !ok {
		return nil, xerrors.Errorf("Unexpected source type: %T", p.transfer.Src)
	}
	// for snapshot only transfers there is no need to tracker
	// we should set in-memory if embedded enabled
	// this allows us to use RO connection for snapshot only transfer
	if p.transfer.SnapshotOnly() && specificConfig.TrackerType == oracle.OracleEmbeddedLogTracker {
		specificConfig.TrackerType = oracle.OracleInMemoryLogTracker
	}
	return NewOracleDataProvider(p.logger, p.registry, p.cp, specificConfig, p.transfer.ID)
}

func (p *Provider) Type() abstract.ProviderType {
	return s3.ProviderType
}

func New(lgr log.Logger, registry metrics.Registry, cp cpclient.Coordinator, transfer *server.Transfer) providers.Provider {
	return &Provider{
		logger:   lgr,
		registry: registry,
		cp:       cp,
		transfer: transfer,
	}
}
