package dblog

import (
	"context"
	"database/sql"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/dblog"
	"github.com/doublecloud/transfer/pkg/dblog/tablequery"
	"go.ytsaurus.tech/library/go/core/log"
)

type Storage struct {
	logger log.Logger

	source  abstract.Source
	storage tablequery.StorageTableQueryable
	db      *sql.DB

	chunkSize uint64

	transferID       string
	represent        dblog.ChangeItemConverter
	keeperSchema     string
	betweenMarksOpts []func()
}

func NewStorage(
	logger log.Logger,
	src abstract.Source,
	storage tablequery.StorageTableQueryable,
	db *sql.DB,
	chunkSize uint64,
	transferID string,
	keeperSchema string,
	represent dblog.ChangeItemConverter,
	betweenMarksOpts ...func(),
) (abstract.Storage, error) {
	return &Storage{
		logger:           log.With(logger, log.Any("component", "dblog")),
		source:           src,
		storage:          storage,
		db:               db,
		chunkSize:        chunkSize,
		transferID:       transferID,
		keeperSchema:     keeperSchema,
		represent:        represent,
		betweenMarksOpts: betweenMarksOpts,
	}, nil
}

func (s *Storage) Close() {
	s.storage.Close()
}

func (s *Storage) Ping() error {
	return s.storage.Ping()
}

func (s *Storage) LoadTable(ctx context.Context, tableDescr abstract.TableDescription, pusher abstract.Pusher) error {
	pkColNames, err := dblog.ResolvePrimaryKeyColumns(ctx, s.storage, tableDescr.ID(), IsSupportedKeyType)
	if err != nil {
		return xerrors.Errorf("unable to get primary key: %w", err)
	}

	chunkSize := s.chunkSize

	if chunkSize == 0 {
		chunkSize, err = dblog.InferChunkSize(s.storage, tableDescr.ID(), dblog.DefaultChunkSizeInBytes)
		if err != nil {
			return xerrors.Errorf("unable to generate chunk size: %w", err)
		}
		s.logger.Infof("Storage::LoadTable - inferred chunkSize: %d", chunkSize)
	} else {
		s.logger.Infof("Storage::LoadTable - from config chunkSize: %d", chunkSize)
	}

	signalTable, err := NewSignalTable(ctx, s.db, s.logger, s.transferID, s.keeperSchema)
	if err != nil {
		return xerrors.Errorf("unable to create signal table: %w", err)
	}

	tableQuery := tablequery.NewTableQuery(tableDescr.ID(), true, "", 0, chunkSize)
	s.logger.Infof("Storage::LoadTable - tableQuery: %v", tableQuery)
	lowBound := signalTable.resolveLowBound(ctx, tableDescr.ID())
	s.logger.Infof("Storage::LoadTable - lowBound: %v", lowBound)

	iterator, err := dblog.NewIncrementalIterator(
		s.logger,
		s.storage,
		tableQuery,
		signalTable,
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

	s.logger.Infof("Storage::LoadTable - first iteration done, extacted items: %d", len(items))

	chunk, err := dblog.ResolveChunkMapFromArr(items, pkColNames, s.represent)
	if err != nil {
		return xerrors.Errorf("failed to resolve chunk: %w", err)
	}

	asyncSink := dblog.NewIncrementalAsyncSink(
		ctx,
		s.logger,
		signalTable,
		tableDescr.ID(),
		iterator,
		pkColNames,
		chunk,
		s.represent,
		func() { s.source.Stop() },
		pusher,
	)

	err = s.source.Run(asyncSink)
	if err != nil {
		return xerrors.Errorf("unable to run worker: %w", err)
	}

	return nil
}

func (s *Storage) TableSchema(ctx context.Context, table abstract.TableID) (*abstract.TableSchema, error) {
	return s.storage.TableSchema(ctx, table)
}

func (s *Storage) TableList(filter abstract.IncludeTableList) (abstract.TableMap, error) {
	return s.storage.TableList(filter)
}

func (s *Storage) ExactTableRowsCount(table abstract.TableID) (uint64, error) {
	return s.storage.ExactTableRowsCount(table)
}

func (s *Storage) EstimateTableRowsCount(table abstract.TableID) (uint64, error) {
	return s.storage.EstimateTableRowsCount(table)
}

func (s *Storage) TableExists(table abstract.TableID) (bool, error) {
	return s.storage.TableExists(table)
}
