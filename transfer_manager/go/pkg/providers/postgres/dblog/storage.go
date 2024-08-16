package dblog

import (
	"context"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/dblog"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/dblog/tablequery"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/postgres"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/stats"
)

type Storage struct {
	pgSource  *postgres.PgSource
	pgStorage *postgres.Storage

	chunkSize uint64

	registry         *stats.SourceStats
	cp               coordinator.Coordinator
	betweenMarksOpts []func()
}

func NewStorage(
	pgSource *postgres.PgSource,
	chunkSize uint64,
	registry *stats.SourceStats,
	cp coordinator.Coordinator,
	betweenMarksOpts ...func(),
) (*Storage, error) {
	pgStorage, err := postgres.NewStorage(pgSource.ToStorageParams(nil))
	if err != nil {
		return nil, xerrors.Errorf("unable to get storage")
	}

	return &Storage{
		pgSource:         pgSource,
		pgStorage:        pgStorage,
		chunkSize:        chunkSize,
		registry:         registry,
		cp:               cp,
		betweenMarksOpts: betweenMarksOpts,
	}, nil
}

func (s *Storage) Close() {
	s.pgStorage.Close()
}

func (s *Storage) Ping() error {
	return s.pgStorage.Ping()
}

func (s *Storage) LoadTable(ctx context.Context, tableDescr abstract.TableDescription, pusher abstract.Pusher) error {
	pkColNames, err := dblog.ResolvePrimaryKeyColumns(ctx, s.pgStorage, tableDescr.ID(), IsSupportedKeyType)
	if err != nil {
		return xerrors.Errorf("unable to get primary key: %w", err)
	}

	chunkSize := s.chunkSize

	if chunkSize == 0 {
		chunkSize, err = dblog.InferChunkSize(s.pgStorage, tableDescr.ID(), dblog.DefaultChunkSizeInBytes)
		if err != nil {
			return xerrors.Errorf("unable to generate chunk size: %w", err)
		}
	}

	slotExist, err := s.slotExist()
	if err != nil {
		return xerrors.Errorf("failed to check existence of a replication slot: %w", err)
	}

	if !slotExist {
		err = postgres.CreateReplicationSlot(s.pgSource)
		if err != nil {
			return xerrors.Errorf("failed to create replication slot: %w", err)
		}
	}

	pgSignalTable, err := newPgSignalTable(ctx, s.pgStorage.Conn, logger.Log, s.pgSource.SlotID)
	if err != nil {
		return xerrors.Errorf("unable to create signal table: %w", err)
	}

	tableQuery := tablequery.NewTableQuery(tableDescr.ID(), true, "", 0, chunkSize)
	lowBound := pgSignalTable.resolveLowBound(ctx, tableDescr.ID())
	iterator, err := dblog.NewIncrementalIterator(
		s.pgStorage,
		tableQuery,
		pgSignalTable,
		postgres.Represent,
		pkColNames,
		lowBound,
		chunkSize,
		s.betweenMarksOpts...,
	)
	if err != nil {
		return xerrors.Errorf("unable to build iterator, err: %w", err)
	}

	src, err := postgres.NewSourceWrapper(
		s.pgSource,
		s.pgSource.SlotID,
		nil,
		logger.Log,
		s.registry,
		s.cp)
	if err != nil {
		return xerrors.Errorf("unable to create source: %w", err)
	}

	items, err := iterator.Next(ctx)
	if err != nil {
		return xerrors.Errorf("failed to do initial iteration: %w", err)
	}

	chunk, err := dblog.ResolveChunkMapFromArr(items, pkColNames, postgres.Represent)
	if err != nil {
		return xerrors.Errorf("failed to resolve chunk: %w", err)
	}

	asyncSink := dblog.NewIncrementalAsyncSink(
		ctx,
		pgSignalTable,
		tableDescr.ID(),
		iterator,
		pkColNames,
		chunk,
		postgres.Represent,
		func() { src.Stop() },
		pusher,
	)

	err = src.Run(asyncSink)
	if err != nil {
		return xerrors.Errorf("unable to run worker: %w", err)
	}

	return nil
}

func (s *Storage) TableSchema(ctx context.Context, table abstract.TableID) (*abstract.TableSchema, error) {
	return s.pgStorage.TableSchema(ctx, table)
}

func (s *Storage) TableList(filter abstract.IncludeTableList) (abstract.TableMap, error) {
	return s.pgStorage.TableList(filter)
}

func (s *Storage) ExactTableRowsCount(table abstract.TableID) (uint64, error) {
	return s.pgStorage.ExactTableRowsCount(table)
}

func (s *Storage) EstimateTableRowsCount(table abstract.TableID) (uint64, error) {
	return s.pgStorage.EstimateTableRowsCount(table)
}

func (s *Storage) TableExists(table abstract.TableID) (bool, error) {
	return s.pgStorage.TableExists(table)
}

func (s *Storage) slotExist() (bool, error) {
	conn, err := postgres.MakeConnPoolFromSrc(s.pgSource, logger.Log)
	if err != nil {
		return false, xerrors.Errorf("failed to create a connection pool: %w", err)
	}
	defer conn.Close()
	slot, err := postgres.NewSlot(conn, logger.Log, s.pgSource)
	if err != nil {
		return false, xerrors.Errorf("failed to create a replication slot object: %w", err)
	}

	return slot.Exist()
}
