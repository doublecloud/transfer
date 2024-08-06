package logbroker

import (
	"context"
	"encoding/gob"
	"os"
	"strings"

	"github.com/doublecloud/tross/library/go/core/metrics"
	"github.com/doublecloud/tross/library/go/core/metrics/solomon"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	cpclient "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/middlewares"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers"
	"go.ytsaurus.tech/library/go/core/log"
)

func init() {
	gob.RegisterName("*server.LfSource", new(LfSource))
	gob.RegisterName("*server.LbSource", new(LbSource))
	gob.RegisterName("*server.LbDestination", new(LbDestination))
	server.RegisterSource(ProviderType, func() server.Source {
		return new(LbSource)
	})
	server.RegisterSource(ProviderWithParserType, func() server.Source {
		return new(LfSource)
	})
	server.RegisterDestination(ProviderType, newDestinationModel)
	abstract.RegisterProviderName(ProviderWithParserType, "Logbroker with parser")
	abstract.RegisterProviderName(ProviderType, "Logbroker")
	providers.Register(ProviderType, New(ProviderType))
	providers.Register(ProviderWithParserType, New(ProviderWithParserType))
}

func newDestinationModel() server.Destination {
	return new(LbDestination)
}

const ProviderWithParserType = abstract.ProviderType("lf")
const ProviderType = abstract.ProviderType("lb")

// To verify providers contract implementation
var (
	_ providers.Replication = (*Provider)(nil)
	_ providers.Sniffer     = (*Provider)(nil)
	_ providers.Sinker      = (*Provider)(nil)

	_ providers.Verifier = (*Provider)(nil)
)

type Provider struct {
	logger   log.Logger
	registry metrics.Registry
	cp       cpclient.Coordinator
	transfer *server.Transfer
	provider abstract.ProviderType
}

func (p *Provider) Sniffer(ctx context.Context) (abstract.Fetchable, error) {
	source, err := p.Source()
	if err != nil {
		return nil, xerrors.Errorf("unable to construct source: %w", err)
	}
	return source.(abstract.Fetchable), nil
}

func (p *Provider) Type() abstract.ProviderType {
	return p.provider
}

func (p *Provider) Source() (abstract.Source, error) {
	switch s := p.transfer.Src.(type) {
	case *LfSource:
		s.IsLbSink = p.transfer.DstType() == ProviderType
		if res, err := NewSource(s, p.logger, p.registry); err != nil {
			return nil, xerrors.Errorf("unable to create new logfeller source: %w", err)
		} else {
			return res, nil
		}
	case *LbSource:
		s.IsLbSink = p.transfer.DstType() == ProviderType
		return NewNativeSource(s, p.logger, p.registry)
	default:
		return nil, xerrors.Errorf("Unknown source type: %T", p.transfer.Src)
	}
}

func (p *Provider) Sink(middlewares.Config) (abstract.Sinker, error) {
	cfg, ok := p.transfer.Dst.(*LbDestination)
	if !ok {
		return nil, xerrors.Errorf("unexpected target type: %T", p.transfer.Dst)
	}
	cfgCopy := *cfg
	cfgCopy.FormatSettings = InferFormatSettings(p.transfer.Src, cfgCopy.FormatSettings)
	return NewReplicationSink(&cfgCopy, p.registry, p.logger, p.transfer.ID)
}

func (p *Provider) SnapshotSink(middlewares.Config) (abstract.Sinker, error) {
	cfg, ok := p.transfer.Dst.(*LbDestination)
	if !ok {
		return nil, xerrors.Errorf("unexpected target type: %T", p.transfer.Dst)
	}
	cfgCopy := *cfg
	cfgCopy.FormatSettings = InferFormatSettings(p.transfer.Src, cfgCopy.FormatSettings)
	return NewSnapshotSink(&cfgCopy, p.registry, p.logger, p.transfer.ID)
}

func (p *Provider) Activate(ctx context.Context, task *server.TransferOperation, table abstract.TableMap, callbacks providers.ActivateCallbacks) error {
	if p.transfer.SrcType() == ProviderType && !p.transfer.IncrementOnly() {
		return xerrors.New("Only allowed mode for Kafka source is replication")
	}
	return nil
}

func (p *Provider) Verify(ctx context.Context) error {
	src, ok := p.transfer.Src.(*LfSource)
	if !ok {
		return nil
	}
	source, err := NewSourceWithRetries(src, p.logger, solomon.NewRegistry(solomon.NewRegistryOpts()), 1)
	if err != nil {
		return xerrors.Errorf("unable to make new logfeller source: %w", err)
	}
	defer source.Stop()
	if src.LfParser && os.Getenv("CGO_ENABLED") == "0" {
		return nil
	}
	sniffer, ok := source.(abstract.Fetchable)
	if !ok {
		return xerrors.Errorf("unexpected source type: %T", source)
	}
	tables, err := sniffer.Fetch()
	if err != nil {
		return xerrors.Errorf("unable to read one from source: %w", err)
	}
	for _, row := range tables {
		if strings.Contains(row.Table, "_unparsed") && len(tables) == 1 {
			return xerrors.New("there is only unparsed in LF sample")
		}
	}
	return nil
}

func New(provider abstract.ProviderType) func(lgr log.Logger, registry metrics.Registry, cp cpclient.Coordinator, transfer *server.Transfer) providers.Provider {
	return func(lgr log.Logger, registry metrics.Registry, cp cpclient.Coordinator, transfer *server.Transfer) providers.Provider {
		return &Provider{
			logger:   lgr,
			registry: registry,
			cp:       cp,
			transfer: transfer,
			provider: provider,
		}
	}
}
