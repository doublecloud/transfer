package init

import (
	"context"

	"github.com/doublecloud/transfer/library/go/core/metrics"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/coordinator"
	"github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/base"
	"github.com/doublecloud/transfer/pkg/middlewares"
	"github.com/doublecloud/transfer/pkg/providers"
	yt_provider "github.com/doublecloud/transfer/pkg/providers/yt"
	ytclient "github.com/doublecloud/transfer/pkg/providers/yt/client"
	ytcopysrc "github.com/doublecloud/transfer/pkg/providers/yt/copy/source"
	"github.com/doublecloud/transfer/pkg/providers/yt/copy/target"
	"github.com/doublecloud/transfer/pkg/providers/yt/lfstaging"
	yt_abstract2 "github.com/doublecloud/transfer/pkg/providers/yt/provider"
	ytsink "github.com/doublecloud/transfer/pkg/providers/yt/sink"
	staticsink "github.com/doublecloud/transfer/pkg/providers/yt/sink/v2"
	ytstorage "github.com/doublecloud/transfer/pkg/providers/yt/storage"
	"github.com/doublecloud/transfer/pkg/targets"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/ypath"
)

func init() {
	providers.Register(yt_provider.ProviderType, New(yt_provider.ProviderType))
	providers.Register(yt_provider.StagingType, New(yt_provider.StagingType))
	providers.Register(yt_provider.CopyType, New(yt_provider.CopyType))
}

// To verify providers contract implementation
var (
	_ providers.Snapshot          = (*Provider)(nil)
	_ providers.Sinker            = (*Provider)(nil)
	_ providers.Abstract2Provider = (*Provider)(nil)
	_ providers.Abstract2Sinker   = (*Provider)(nil)

	_ providers.Cleanuper  = (*Provider)(nil)
	_ providers.TMPCleaner = (*Provider)(nil)
	_ providers.Verifier   = (*Provider)(nil)
)

type Provider struct {
	logger   log.Logger
	registry metrics.Registry
	cp       coordinator.Coordinator
	transfer *model.Transfer
	provider abstract.ProviderType
}

func (p *Provider) Target(...abstract.SinkOption) (base.EventTarget, error) {
	dst, ok := p.transfer.Dst.(*yt_provider.YtCopyDestination)
	if !ok {
		return nil, targets.UnknownTargetError
	}
	return target.NewTarget(p.logger, p.registry, dst, p.transfer.ID)
}

func (p *Provider) Verify(ctx context.Context) error {
	dst, ok := p.transfer.Dst.(yt_provider.YtDestinationModel)
	if !ok {
		return nil
	}
	dst.SetSnapshotLoad()
	if dst.Static() && !p.transfer.SnapshotOnly() {
		return xerrors.New("static yt available only for snapshot copy")
	}
	return nil
}

func (p *Provider) Storage() (abstract.Storage, error) {
	src, ok := p.transfer.Src.(*yt_provider.YtSource)
	if !ok {
		return nil, xerrors.Errorf("unexpected target type: %T", p.transfer.Dst)
	}
	return ytstorage.NewStorage(&yt_provider.YtStorageParams{
		Token:                 src.YtToken,
		Cluster:               src.Cluster,
		Path:                  src.Paths[0], // TODO: Handle multi-path in abstract 1 yt storage
		Spec:                  nil,
		DisableProxyDiscovery: src.Connection.DisableProxyDiscovery,
	})
}

func (p *Provider) DataProvider() (provider base.DataProvider, err error) {
	specificConfig, ok := p.transfer.Src.(*yt_provider.YtSource)
	if !ok {
		return nil, xerrors.Errorf("Unexpected source type: %T", p.transfer.Src)
	}
	if _, ok := p.transfer.Dst.(*yt_provider.YtCopyDestination); ok {
		provider, err = ytcopysrc.NewSource(p.logger, p.registry, specificConfig, p.transfer.ID)
	} else {
		provider, err = yt_abstract2.NewSource(p.logger, p.registry, specificConfig)
	}
	return provider, err
}

func (p *Provider) Type() abstract.ProviderType {
	return p.provider
}

func (p *Provider) Sink(middlewares.Config) (abstract.Sinker, error) {
	if p.provider == yt_provider.StagingType {
		dst, ok := p.transfer.Dst.(*yt_provider.LfStagingDestination)
		if !ok {
			return nil, xerrors.Errorf("unexpected target type: %T", p.transfer.Dst)
		}
		s, err := lfstaging.NewSinker(dst, getJobIndex(p.transfer), p.transfer, p.logger)
		if err != nil {
			return nil, xerrors.Errorf("failed to create lf staging sinker: %s", err)
		}
		return s, nil
	}
	dst, ok := p.transfer.Dst.(*yt_provider.YtDestinationWrapper)
	if !ok {
		return nil, xerrors.Errorf("unexpected target type: %T", p.transfer.Dst)
	}
	var s abstract.Sinker
	var err error
	if dst.Static() {
		if !p.transfer.SnapshotOnly() {
			return nil, xerrors.Errorf("failed to create YT (static) sinker: can't make '%s' transfer while sinker is static", p.transfer.Type)
		}

		if dst.Rotation() != nil {
			if s, err = ytsink.NewRotatedStaticSink(dst, p.registry, p.logger, p.cp, p.transfer.ID); err != nil {
				return nil, xerrors.Errorf("failed to create YT (static) sinker: %w", err)
			}
		} else {
			if s, err = staticsink.NewStaticSink(dst, p.cp, p.transfer.ID, p.registry, p.logger); err != nil {
				return nil, xerrors.Errorf("failed to create YT (static) sinker: %w", err)
			}
		}
	} else {
		jobIndex := getJobIndex(p.transfer)
		s, err = ytsink.NewSinker(dst, p.transfer.ID, jobIndex, p.logger, p.registry, p.cp, p.transfer.TmpPolicy)
		if err != nil {
			return nil, xerrors.Errorf("failed to create YT (non-static) sinker: %w", err)
		}
	}
	return s, nil
}

func getJobIndex(transfer *model.Transfer) int {
	if shardingTaskRuntime, ok := transfer.Runtime.(abstract.ShardingTaskRuntime); ok {
		return shardingTaskRuntime.CurrentJobIndex()
	} else {
		return 0
	}
}

func (p *Provider) TMPCleaner(ctx context.Context, task *model.TransferOperation) (providers.Cleaner, error) {
	dst, ok := p.transfer.Dst.(yt_provider.YtDestinationModel)
	if !ok {
		return nil, xerrors.Errorf("unexpected destincation type: %T", p.transfer.Dst)
	}
	return yt_provider.NewTmpCleaner(dst, p.logger)
}

func (p *Provider) Cleanup(ctx context.Context, task *model.TransferOperation) error {
	dst, ok := p.transfer.Dst.(yt_provider.YtDestinationModel)
	if !ok {
		return xerrors.Errorf("unexpected dst type: %T", p.transfer.Dst)
	}
	if dst.UseStaticTableOnSnapshot() {
		ytClient, err := ytclient.FromConnParams(dst, p.logger)
		if err != nil {
			return xerrors.Errorf("unable to initialize yt client: %w", err)
		}

		if err := ytsink.CleanupSingleStaticTable(ctx, ytClient, p.logger, ypath.Path(dst.Path()), p.transfer.ID); err != nil {
			return xerrors.Errorf("unable to cleanup yt destination (static table on snapshot)")
		}
	}
	return nil
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
