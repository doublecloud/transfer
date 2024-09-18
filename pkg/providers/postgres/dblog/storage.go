package dblog

import (
	"context"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/dblog"
	"github.com/doublecloud/transfer/pkg/dblog/tablequery"
	"github.com/jackc/pgx/v4/pgxpool"
)

type Storage struct {
	src       abstract.Source
	pgStorage tablequery.StorageTableQueryable
	conn      *pgxpool.Pool

	chunkSize uint64

	transferID       string
	represent        dblog.ChangeItemConverter
	betweenMarksOpts []func()
}

func NewStorage(
	src abstract.Source,
	pgStorage tablequery.StorageTableQueryable,
	conn *pgxpool.Pool,
	chunkSize uint64,
	transferID string,
	represent dblog.ChangeItemConverter,
	betweenMarksOpts ...func(),
) (abstract.Storage, error) {
	return &Storage{
		src:              src,
		pgStorage:        pgStorage,
		conn:             conn,
		chunkSize:        chunkSize,
		transferID:       transferID,
		represent:        represent,
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

	pgSignalTable, err := newPgSignalTable(ctx, s.conn, logger.Log, s.transferID)
	if err != nil {
		return xerrors.Errorf("unable to create signal table: %w", err)
	}

	tableQuery := tablequery.NewTableQuery(tableDescr.ID(), true, "", 0, chunkSize)
	lowBound := pgSignalTable.resolveLowBound(ctx, tableDescr.ID())
	iterator, err := dblog.NewIncrementalIterator(
		s.pgStorage,
		tableQuery,
		pgSignalTable,
		s.represent,
		pkColNames,
		lowBound,
		chunkSize,
		s.betweenMarksOpts...,
	)
	if err != nil {
		return xerrors.Errorf("unable to build iterator, err: %w", err)
	}

	items, err := iterator.Next(ctx)
	if err != nil {
		return xerrors.Errorf("failed to do initial iteration: %w", err)
	}

	chunk, err := dblog.ResolveChunkMapFromArr(items, pkColNames, s.represent)
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
		s.represent,
		func() { s.src.Stop() },
		pusher,
	)

	err = s.src.Run(asyncSink)
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
