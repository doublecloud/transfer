package postgres

import (
	"context"
	"encoding/gob"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/doublecloud/tross/library/go/core/metrics"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/errors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/errors/categories"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/middlewares"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

func init() {
	gob.RegisterName("*server.PgSource", new(PgSource))
	gob.RegisterName("*server.PgDestination", new(PgDestination))
	server.RegisterDestination(ProviderType, func() server.Destination {
		return new(PgDestination)
	})
	server.RegisterSource(ProviderType, func() server.Source {
		return new(PgSource)
	})

	abstract.RegisterProviderName(ProviderType, "PostgreSQL")
	providers.Register(ProviderType, New)

	/*
		"__consumer_keeper":
			consumer TEXT, locked_till TIMESTAMPTZ, locked_by TEXT
			Table in which we regularly write something. The fact of writing is important here, not the data itself.
			The problem that we solve is if there is no record in the cluster, then we do not receive any events
			from the slot, and we do not commit any progress of reading replication slot.
			And if we don't commit any progress, then WAL accumulates.

		"__data_transfer_lsn":
			transfer_id TEXT, schema_name TEXT, table_name TEXT, lsn BIGINT
			Table (in target) needed for resolving data overlapping during SNAPSHOT_AND_INCREMENT transfers.

		"__data_transfer_mole_finder":
			slot_id TEXT,
			iterator INT,
			replicated_iterator INT,
			last_iterator_update TIMESTAMP,
			last_replicated_update TIMESTAMP,
			committed_lsn TEXT

			Table for storing replication slot's progress (lsn position).
	*/
	abstract.RegisterSystemTables(TableConsumerKeeper, TableMoleFinder, TableLSN)
}

const (
	TableConsumerKeeper = abstract.TableConsumerKeeper // "__consumer_keeper"
	TableMoleFinder     = "__data_transfer_mole_finder"
	TableLSN            = abstract.TableLSN // "__data_transfer_lsn"
)

const ProviderType = abstract.ProviderType("pg")

// To verify providers contract implementation
var (
	_ providers.Sampleable  = (*Provider)(nil)
	_ providers.Snapshot    = (*Provider)(nil)
	_ providers.Replication = (*Provider)(nil)
	_ providers.Sinker      = (*Provider)(nil)
	_ providers.Verifier    = (*Provider)(nil)
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

func (p *Provider) Cleanup(ctx context.Context, task *server.TransferOperation) error {
	src, ok := p.transfer.Src.(*PgSource)
	if !ok {
		return xerrors.Errorf("unexpected type: %T", p.transfer.Src)
	}
	p.fillParams(src)
	if p.transfer.SnapshotOnly() {
		return nil
	}
	if !p.transfer.SnapshotOnly() {
		if err := DropReplicationSlot(src); err != nil {
			return xerrors.Errorf("Unable to drop replication slot: %w", err)
		}
	}
	return nil
}

func (p *Provider) Deactivate(ctx context.Context, task *server.TransferOperation) error {
	src, ok := p.transfer.Src.(*PgSource)
	if !ok {
		return xerrors.Errorf("unexpected type: %T", p.transfer.Src)
	}
	p.fillParams(src)
	if p.transfer.SnapshotOnly() {
		return nil
	}
	if err := DropReplicationSlot(src); err != nil {
		return xerrors.Errorf("Unable to drop replication slot: %w", err)
	}

	if !p.transfer.IncrementOnly() && src.PostSteps.AnyStepIsTrue() {
		pgdump, err := ExtractPgDumpSchema(p.transfer)
		if err != nil {
			return xerrors.Errorf("failed to extract schema from source: %w", err)
		}
		if err := ApplyPgDumpPostSteps(pgdump, p.transfer, p.registry); err != nil {
			return xerrors.Errorf("failed to apply pre-steps to transfer schema: %w", err)
		}
	}
	return nil
}

func (p *Provider) Activate(ctx context.Context, task *server.TransferOperation, tables abstract.TableMap, callbacks providers.ActivateCallbacks) error {
	src, ok := p.transfer.Src.(*PgSource)
	if !ok {
		return xerrors.Errorf("unexpected type: %T", p.transfer.Src)
	}
	p.fillParams(src)
	if err := VerifyPostgresTables(src, p.transfer, p.logger); err != nil {
		if IsPKeyCheckError(err) {
			if !p.transfer.SnapshotOnly() {
				return xerrors.Errorf("some tables have no PRIMARY KEY. This is allowed for Snapshot-only transfers. Error: %w", err)
			}
			logger.Log.Warn("Some tables have no PRIMARY KEY. This is allowed for Snapshot-only transfers", log.Error(err))
		} else {
			return xerrors.Errorf("tables verification failed: %w", err)
		}
	}
	p.logger.Info("Preparing PostgreSQL source")
	if !p.transfer.SnapshotOnly() {
		if err := CreateReplicationSlot(src); err != nil {
			return xerrors.Errorf("failed to create a replication slot %q at source: %w", src.SlotID, err)
		}
		callbacks.Rollbacks.Add(func() {
			if err := DropReplicationSlot(src); err != nil {
				logger.Log.Error("Unable to drop replication slot", log.Error(err), log.String("slot_name", src.SlotID))
			}
		})
	}
	if !p.transfer.IncrementOnly() {
		if err := callbacks.Cleanup(tables); err != nil {
			return xerrors.Errorf("failed to cleanup sink: %w", err)
		}
	}
	if src.PreSteps.AnyStepIsTrue() {
		pgdump, err := ExtractPgDumpSchema(p.transfer)
		if err != nil {
			return xerrors.Errorf("failed to extract schema from source: %w", err)
		}
		if err := ApplyPgDumpPreSteps(pgdump, p.transfer, p.registry); err != nil {
			return xerrors.Errorf("failed to apply pre-steps to transfer schema: %w", err)
		}
	}
	if !p.transfer.IncrementOnly() {
		if err := callbacks.CheckIncludes(tables); err != nil {
			return xerrors.Errorf("Failed in accordance with configuration: %w", err)
		}
		if err := callbacks.Upload(tables); err != nil {
			return xerrors.Errorf("Snapshot loading failed: %w", err)
		}
	}
	if p.transfer.SnapshotOnly() && src.PostSteps.AnyStepIsTrue() {
		pgdump, err := ExtractPgDumpSchema(p.transfer)
		if err != nil {
			return xerrors.Errorf("failed to extract schema from source: %w", err)
		}
		if err := ApplyPgDumpPostSteps(pgdump, p.transfer, p.registry); err != nil {
			return xerrors.Errorf("failed to apply post-steps to transfer schema: %w", err)
		}
	}
	return nil
}

func (p *Provider) Verify(ctx context.Context) error {
	src, ok := p.transfer.Src.(*PgSource)
	if !ok {
		return xerrors.Errorf("unexpected type: %T", p.transfer.Src)
	}
	p.fillParams(src)
	if src.SubNetworkID != "" {
		return xerrors.New("unable to verify derived network")
	}
	if err := VerifyPostgresTables(src, p.transfer, p.logger); err != nil {
		if IsPKeyCheckError(err) && p.transfer.SnapshotOnly() {
			logger.Log.Warnf("Some tables dont have primary key but it is still allowed for snapshot only transfers: %v", err)
		} else {
			return xerrors.Errorf("unable to verify postgres tables: %w", err)
		}
	}
	if !p.transfer.SnapshotOnly() {
		if err := CreateReplicationSlot(src); err == nil {
			if err := DropReplicationSlot(src); err != nil {
				return xerrors.Errorf("unable to drop replication slot: %w", err)
			}
		} else {
			return xerrors.Errorf("unable to create replication slot: %w", err)
		}
	}
	return nil
}

func (p *Provider) Sink(config middlewares.Config) (abstract.Sinker, error) {
	dst, ok := p.transfer.Dst.(*PgDestination)
	if !ok {
		return nil, xerrors.Errorf("unexpected type: %T", p.transfer.Src)
	}
	if p.transfer.Type == abstract.TransferTypeSnapshotOnly {
		dst.PerTransactionPush = false
	}

	isHomo := p.transfer.SrcType() == ProviderType
	if !isHomo && !dst.MaintainTables {
		dst.MaintainTables = true
	}
	s, err := NewSink(p.logger, p.transfer.ID, dst.ToSinkParams(), p.registry)
	if err != nil {
		return nil, xerrors.Errorf("failed to create PostgreSQL sinker: %w", err)
	}
	return s, nil
}

func (p *Provider) Source() (abstract.Source, error) {
	s, ok := p.transfer.Src.(*PgSource)
	if !ok {
		return nil, xerrors.Errorf("unexpected type: %T", p.transfer.Src)
	}
	p.fillParams(s)
	var src abstract.Source
	st := stats.NewSourceStats(p.registry)
	if err := backoff.Retry(func() error {
		if source, err := NewSourceWrapper(s, p.transfer.ID, p.transfer.DataObjects, p.logger, st, p.cp); err != nil {
			p.logger.Error("unable to init", log.Error(err))
			return xerrors.Errorf("unable to create new pg source: %w", err)
		} else {
			src = source
		}
		return nil
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)); err != nil {
		return nil, err
	}
	return src, nil
}

// Build a type mapping and print elapsed time in log.
func buildTypeMapping(ctx context.Context, storage *Storage) (TypeNameToOIDMap, error) {
	startTime := time.Now()
	logger.Log.Info("Building type map for the destination database")
	typeMapping, err := storage.BuildTypeMapping(ctx)

	logger.Log.Info("Built type map for the destination database", log.Duration("elapsed", time.Since(startTime)), log.Int("entries_count", len(typeMapping)), log.Error(err))
	return typeMapping, err
}

func (p *Provider) Storage() (abstract.Storage, error) {
	src, ok := p.transfer.Src.(*PgSource)
	if !ok {
		return nil, xerrors.Errorf("unexpected type: %T", p.transfer.Src)
	}
	p.fillParams(src)
	opts := []StorageOpt{WithMetrics(p.registry)}
	pgDst, ok := p.transfer.Dst.(*PgDestination)
	if ok {
		dstStorage, err := NewStorage(pgDst.ToStorageParams())
		if err != nil {
			return nil, errors.CategorizedErrorf(categories.Target, "failed to connect to the destination cluster to get type information: %w", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		typeMap, err := buildTypeMapping(ctx, dstStorage)
		if err != nil {
			return nil, errors.CategorizedErrorf(categories.Target, "failed to build destination database type mapping: %w", err)
		}
		opts = append(opts, WithTypeMapping(typeMap))
	}
	storage, err := NewStorage(src.ToStorageParams(p.transfer), opts...)
	if err != nil {
		return nil, xerrors.Errorf("failed to create a PostgreSQL storage: %w", err)
	}
	storage.IsHomo = src.IsHomo
	if p.transfer.DataObjects != nil && len(p.transfer.DataObjects.IncludeObjects) > 0 {
		storage.loadDescending = src.CollapseInheritTables // For include objects we force to load parent table with all their children
	}
	return storage, nil
}

func (p *Provider) fillParams(src *PgSource) {
	if src.SlotID == "" {
		src.SlotID = p.transfer.ID
	}
	if !src.IsHomo {
		src.IsHomo = p.transfer.DstType() == ProviderType
	}
	if src.NoHomo {
		src.IsHomo = false
	}
}

func (p *Provider) SourceSampleableStorage() (abstract.SampleableStorage, []abstract.TableDescription, error) {
	src, ok := p.transfer.Src.(*PgSource)
	p.fillParams(src)
	if !ok {
		return nil, nil, xerrors.Errorf("unexpected type: %T", p.transfer.Src)
	}
	srcStorage, err := NewStorage(src.ToStorageParams(p.transfer))
	if err != nil {
		return nil, nil, xerrors.Errorf(`unable to create pg storage: %w`, err)
	}
	defer srcStorage.Close()
	if _, ok := p.transfer.Dst.(*PgDestination); ok {
		srcStorage.IsHomo = true // Need to toggle is homo to exclude view and mat views
	}
	all, err := srcStorage.TableList(nil)
	if err != nil {
		return nil, nil, xerrors.Errorf(`unable to get table map from source storage: %w`, err)
	}
	var tables []abstract.TableDescription
	for tID, tInfo := range all {
		if tID.Name == TableConsumerKeeper {
			continue
		}
		if src.Include(tID) {
			tables = append(tables, abstract.TableDescription{
				Name:   tID.Name,
				Schema: tID.Namespace,
				Filter: "",
				EtaRow: tInfo.EtaRow,
				Offset: 0,
			})
		}
	}
	return srcStorage, tables, nil
}

func (p *Provider) DestinationSampleableStorage() (abstract.SampleableStorage, error) {
	dst, ok := p.transfer.Dst.(*PgDestination)
	if !ok {
		return nil, xerrors.Errorf("unexpected type: %T", p.transfer.Src)
	}
	return NewStorage(dst.ToStorageParams())
}

func (p *Provider) Type() abstract.ProviderType {
	return ProviderType
}

func New(lgr log.Logger, registry metrics.Registry, cp coordinator.Coordinator, transfer *server.Transfer) providers.Provider {
	return &Provider{
		logger:   lgr,
		registry: registry,
		cp:       cp,
		transfer: transfer,
	}
}
