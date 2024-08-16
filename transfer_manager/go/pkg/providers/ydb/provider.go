package ydb

import (
	"context"
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
	gob.RegisterName("*server.YdbDestination", new(YdbDestination))
	gob.RegisterName("*server.YdbSource", new(YdbSource))
	server.RegisterDestination(ProviderType, func() server.Destination {
		return new(YdbDestination)
	})
	server.RegisterSource(ProviderType, func() server.Source {
		return new(YdbSource)
	})

	abstract.RegisterProviderName(ProviderType, "YDB")
	providers.Register(ProviderType, New)
}

const ProviderType = abstract.ProviderType("ydb")

// To verify providers contract implementation
var (
	_ providers.Snapshot    = (*Provider)(nil)
	_ providers.Replication = (*Provider)(nil)
	_ providers.Sinker      = (*Provider)(nil)

	_ providers.Activator   = (*Provider)(nil)
	_ providers.Deactivator = (*Provider)(nil)
	_ providers.Cleanuper   = (*Provider)(nil)
)

type Provider struct {
	logger   log.Logger
	registry metrics.Registry
	cp       coordinator.Coordinator
	transfer *server.Transfer
}

func (p *Provider) Storage() (abstract.Storage, error) {
	src, ok := p.transfer.Src.(*YdbSource)
	if !ok {
		return nil, xerrors.Errorf("unexpected target type: %T", p.transfer.Dst)
	}
	return NewStorage(src.ToStorageParams())
}

func (p *Provider) Source() (abstract.Source, error) {
	src, ok := p.transfer.Src.(*YdbSource)
	if !ok {
		return nil, xerrors.Errorf("Unknown source type: %T", p.transfer.Src)
	}
	err := CreateChangeFeedIfNotExists(src, p.transfer.ID)
	if err != nil {
		return nil, xerrors.Errorf("unable to upsert changeFeed, err: %w", err)
	}
	return NewSource(p.transfer.ID, src, p.logger, p.registry)
}

func (p *Provider) Activate(ctx context.Context, task *server.TransferOperation, tables abstract.TableMap, callbacks providers.ActivateCallbacks) error {
	src, ok := p.transfer.Src.(*YdbSource)
	if !ok {
		return xerrors.Errorf("unexpected src type: %T", p.transfer.Src)
	}
	if !p.transfer.SnapshotOnly() {
		err := DropChangeFeed(src, p.transfer.ID)
		if err != nil {
			return xerrors.Errorf("unable to drop changeFeed, err: %w", err)
		}
		err = CreateChangeFeed(src, p.transfer.ID)
		if err != nil {
			return xerrors.Errorf("unable to create changeFeed, err: %w", err)
		}
	}
	if !p.transfer.IncrementOnly() {
		if err := callbacks.Cleanup(ConvertTableMapToYDBRelPath(src.ToStorageParams(), tables)); err != nil {
			return xerrors.Errorf("Sinker cleanup failed: %w", err)
		}
		if err := callbacks.CheckIncludes(tables); err != nil {
			return xerrors.Errorf("Failed in accordance with configuration: %w", err)
		}
		if err := callbacks.Upload(tables); err != nil {
			return xerrors.Errorf("Snapshot loading failed: %w", err)
		}
	}
	return nil
}

func (p *Provider) Deactivate(ctx context.Context, task *server.TransferOperation) error {
	src, ok := p.transfer.Src.(*YdbSource)
	if !ok {
		return xerrors.Errorf("unexpected src type: %T", p.transfer.Src)
	}
	if !p.transfer.SnapshotOnly() {
		err := DropChangeFeed(src, p.transfer.ID)
		if err != nil {
			return xerrors.Errorf("drop changefeed error occurred: %w", err)
		}
	}
	return nil
}

func (p *Provider) Cleanup(ctx context.Context, task *server.TransferOperation) error {
	src, ok := p.transfer.Src.(*YdbSource)
	if !ok {
		return xerrors.Errorf("unexpected src type: %T", p.transfer.Src)
	}
	return DropChangeFeed(src, p.transfer.ID)
}

func (p *Provider) Type() abstract.ProviderType {
	return ProviderType
}

func (p *Provider) Sink(middlewares.Config) (abstract.Sinker, error) {
	dst, ok := p.transfer.Dst.(*YdbDestination)
	if !ok {
		return nil, xerrors.Errorf("unexpected target type: %T", p.transfer.Dst)
	}
	return NewSinker(p.logger, dst, p.registry)
}

func New(lgr log.Logger, registry metrics.Registry, cp coordinator.Coordinator, transfer *server.Transfer) providers.Provider {
	return &Provider{
		logger:   lgr,
		registry: registry,
		cp:       cp,
		transfer: transfer,
	}
}
