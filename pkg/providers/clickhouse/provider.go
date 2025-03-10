package clickhouse

import (
	"context"
	"encoding/gob"
	"time"

	"github.com/doublecloud/transfer/library/go/core/metrics"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/coordinator"
	dp_model "github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/base"
	"github.com/doublecloud/transfer/pkg/data"
	"github.com/doublecloud/transfer/pkg/middlewares"
	"github.com/doublecloud/transfer/pkg/providers"
	ch_async_sink "github.com/doublecloud/transfer/pkg/providers/clickhouse/async"
	"github.com/doublecloud/transfer/pkg/providers/clickhouse/httpclient"
	"github.com/doublecloud/transfer/pkg/providers/clickhouse/model"
	sink_registry "github.com/doublecloud/transfer/pkg/sink"
	"github.com/doublecloud/transfer/pkg/targets"
	"go.ytsaurus.tech/library/go/core/log"
)

func init() {
	gob.RegisterName("*server.ChSource", new(model.ChSource))
	gob.RegisterName("*server.ChDestination", new(model.ChDestination))
	dp_model.RegisterDestination(ProviderType, func() dp_model.Destination {
		return new(model.ChDestination)
	})
	dp_model.RegisterSource(ProviderType, func() dp_model.Source {
		return new(model.ChSource)
	})

	abstract.RegisterProviderName(ProviderType, "ClickHouse")
	providers.Register(ProviderType, New)
}

const ProviderType = abstract.ProviderType("ch")

// To verify providers contract implementation
var (
	_ providers.Snapshot          = (*Provider)(nil)
	_ providers.Abstract2Provider = (*Provider)(nil)
	_ providers.AsyncSinker       = (*Provider)(nil)
	_ providers.Sinker            = (*Provider)(nil)
	_ providers.Abstract2Sinker   = (*Provider)(nil)
	_ providers.Tester            = (*Provider)(nil)
	_ providers.Activator         = (*Provider)(nil)
)

type Provider struct {
	logger   log.Logger
	registry metrics.Registry
	cp       coordinator.Coordinator
	transfer *dp_model.Transfer
}

func (p *Provider) Target(options ...abstract.SinkOption) (base.EventTarget, error) {
	if _, ok := p.transfer.Src.(*model.ChSource); !ok {
		return nil, targets.UnknownTargetError
	}
	return NewHTTPTarget(p.transfer, p.registry, p.logger)
}

func (p *Provider) Sink(config middlewares.Config) (abstract.Sinker, error) {
	s, err := NewSink(p.transfer, p.logger, p.registry, p.transfer.Runtime, config)
	if err != nil {
		return nil, xerrors.Errorf("failed to create ClickHouse sinker: %w", err)
	}
	dst, ok := p.transfer.Dst.(*model.ChDestination)
	if !ok {
		return nil, xerrors.Errorf("unexpected source type: %T", p.transfer.Src)
	}
	if dst.ExactlyOnce {
		db, err := makeShardConnection(dst.ToStorageParams(), "")
		if err != nil {
			return nil, xerrors.Errorf("unable to init keeper db connection: %w", err)
		}
		keeperStore, err := NewKeeperBlockStore("exactly_once_keeper", db)
		if err != nil {
			return nil, xerrors.Errorf("unable to init keeper block store: %w", err)
		}
		return NewExactlyOnce(s, keeperStore, log.With(p.logger, log.Any("component", "exactly_once"))), nil
	}
	return s, nil
}

func (p *Provider) AsyncSink(middleware abstract.Middleware) (abstract.AsyncSink, error) {
	if p.transfer.IsAsyncCHExp() {
		p.logger.Warn("Using experimental asynchronous ClickHouse sink")
		sink, err := ch_async_sink.NewSink(p.transfer, p.transfer.Dst.(*model.ChDestination), p.logger, p.registry, middleware)
		if err != nil {
			return nil, xerrors.Errorf("error getting experimental asynchronous ClickHouse sink: %w", err)
		}
		return sink, nil
	}
	return nil, sink_registry.NoAsyncSinkErr
}

func (p *Provider) Storage() (abstract.Storage, error) {
	src, ok := p.transfer.Src.(*model.ChSource)
	if !ok {
		return nil, xerrors.Errorf("unexpected source type: %T", p.transfer.Src)
	}
	chOpts := []StorageOpt{WithMetrics(p.registry), WithTableFilter(src)}
	if _, ok := p.transfer.Dst.(*model.ChDestination); ok {
		chOpts = append(chOpts, WithHomo())
	}
	storage, err := NewStorage(src.ToStorageParams(), p.transfer, chOpts...)
	if err != nil {
		return nil, xerrors.Errorf("failed to create a ClickHouse storage: %w", err)
	}
	return storage, nil
}

func (p *Provider) DataProvider() (base.DataProvider, error) {
	specificConfig, ok := p.transfer.Src.(*model.ChSource)
	if !ok {
		return nil, xerrors.Errorf("Unexpected source type: %T", p.transfer.Src)
	}
	if p.transfer.DstType() != ProviderType {
		return nil, data.TryLegacySourceError // just for homo
	}
	return NewClickhouseProvider(p.logger, p.registry, specificConfig, p.transfer)
}

func (p *Provider) Activate(_ context.Context, _ *dp_model.TransferOperation, tables abstract.TableMap, callbacks providers.ActivateCallbacks) error {
	if !p.transfer.SnapshotOnly() {
		return xerrors.New("Only allowed mode for CH source is snapshot")
	}
	if err := callbacks.Cleanup(tables); err != nil {
		return xerrors.Errorf("Sinker cleanup failed: %w", err)
	}
	if err := callbacks.CheckIncludes(tables); err != nil {
		return xerrors.Errorf("Failed in accordance with configuration: %w", err)
	}
	if err := p.loadClickHouseSchema(); err != nil {
		return xerrors.Errorf("Cannot load schema from source database: %w", err)
	}
	if err := callbacks.Upload(tables); err != nil {
		return xerrors.Errorf("Snapshot loading failed: %w", err)
	}
	return nil
}

func (p *Provider) Type() abstract.ProviderType {
	return ProviderType
}

const (
	CredentialsCheckType = abstract.CheckType("credentials")
	ConnectivityNative   = abstract.CheckType("connection-native")
	ConnectivityHTTP     = abstract.CheckType("connection-http")
)

func (p *Provider) TestChecks() []abstract.CheckType {
	return []abstract.CheckType{ConnectivityHTTP, ConnectivityNative, CredentialsCheckType}
}

func (p *Provider) Test(ctx context.Context) *abstract.TestResult {
	src, ok := p.transfer.Src.(*model.ChSource)
	if !ok {
		return nil
	}

	tr := abstract.NewTestResult(p.TestChecks()...)
	// Native connect
	db, err := MakeConnection(src.ToStorageParams())
	if err != nil {
		return tr.NotOk(ConnectivityNative, xerrors.Errorf("unable to init a CH storage: %w", err))
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		return tr.NotOk(ConnectivityNative, xerrors.Errorf("unable to reach ClickHouse: %w", err))
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return tr.NotOk(ConnectivityNative, xerrors.Errorf("unable to establish a connection to ClickHouse: %w", err))
	}
	defer conn.Close()

	err = conn.PingContext(ctx)
	if err != nil {
		return tr.NotOk(ConnectivityNative, xerrors.Errorf("unable to ping a connection to ClickHouse: %w", err))
	}

	shortContext, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	rows, err := conn.QueryContext(shortContext, "SELECT 1;")
	if err != nil || rows == nil {
		return tr.NotOk(CredentialsCheckType, xerrors.Errorf("unable to query ClickHouse: %w", err))
	}
	defer rows.Close()
	tr.Ok(CredentialsCheckType)

	err = conn.Close()
	if err != nil {
		return tr.NotOk(ConnectivityNative, xerrors.Errorf("unable to close a connection to ClickHouse: %w", err))
	}

	tr.Ok(ConnectivityNative)

	// HTTP connect
	cl, err := httpclient.NewHTTPClientImpl(src.ToStorageParams().ToConnParams())
	if err != nil {
		return tr.NotOk(ConnectivityHTTP, xerrors.Errorf("unable to create ClickHouse client: %w", err))
	}
	var res uint64
	shards := src.ToSinkParams().Shards()

	for _, shardHosts := range shards {
		for _, host := range shardHosts {
			err = cl.Query(context.Background(), p.logger, host, "SELECT 1;", &res)
			if err != nil {
				return tr.NotOk(ConnectivityHTTP, xerrors.Errorf("unable to query ClickHouse host: %s err: %w", host, err))
			}
			p.logger.Infof("host is reachable! host: %s", host)
		}
	}

	tr.Ok(ConnectivityHTTP)
	return tr
}

func New(lgr log.Logger, registry metrics.Registry, cp coordinator.Coordinator, transfer *dp_model.Transfer) providers.Provider {
	return &Provider{
		logger:   lgr,
		registry: registry,
		cp:       cp,
		transfer: transfer,
	}
}
