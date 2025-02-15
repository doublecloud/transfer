package mysql

import (
	"context"
	"fmt"
	"math/big"

	"github.com/dustin/go-humanize"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/dblog/tablequery"
	"github.com/doublecloud/transfer/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

func (s *Storage) LoadQueryTable(ctx context.Context, tableQuery tablequery.TableQuery, pusher abstract.Pusher) error {
	table := abstract.TableDescription{
		Name:   tableQuery.TableID.Name,
		Schema: tableQuery.TableID.Namespace,
		EtaRow: 0,
		Filter: tableQuery.Filter,
		Offset: tableQuery.Offset,
	}
	st := util.GetTimestampFromContextOrNow(ctx)

	pos, err := s.Position(ctx)
	if err != nil {
		return xerrors.Errorf("unable to read log position: %w", err)
	}

	tx, rollbacks, err := s.getSnapshotQueryable(ctx)
	if err != nil {
		return xerrors.Errorf("Can't get table read transaction: %w", err)
	}
	defer rollbacks.Do()

	currTableSchema := s.fqtnSchema[table.ID()]

	querySelect := buildSelectQuery(table, currTableSchema.Columns())

	logger.Log.Infof("Storage::LoadQueryTable - built query: %s", querySelect)
	logger.Log.Info("Storage read table", log.String("table", table.Fqtn()), log.String("query", querySelect))

	if _, err := tx.ExecContext(ctx, "set net_read_timeout=3600;"); err != nil {
		return xerrors.Errorf("Unable to set net_read_timeout=3600: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "set net_write_timeout=3600;"); err != nil {
		return xerrors.Errorf("Unable to set net_write_timeout=3600: %w", err)
	}
	timezone := timezoneOffset(s.ConnectionParams.Location)
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("set time_zone = '%s';", timezone)); err != nil {
		return xerrors.Errorf("unable to set session timezone %s: %w", timezone, err)
	}

	tableRowsCount, tableDataSizeInBytes, err := getTableRowsCountTableDataSizeInBytes(ctx, tx, table)
	if err != nil {
		return xerrors.Errorf("Unable to get table %v size: %w", table.Fqtn(), err)
	}

	chunkSize := calcChunkSize(tableRowsCount, tableDataSizeInBytes, 100000)

	logger.Log.Infof("Table %v: total rows %v, size %v in chunks by %v", table.Fqtn(), tableRowsCount, humanize.BigBytes(big.NewInt(int64(tableDataSizeInBytes))), chunkSize)

	if table.Offset == 0 && table.Filter == "" && s.preSteps.Tables && s.IsHomo {
		if err := pushCreateTable(ctx, tx, table.ID(), st, pusher); err != nil {
			return xerrors.Errorf("Unable to push drop and create table %v DDL: %w", table.Fqtn(), err)
		}
	}

	colNameToColTypeName, err := makeMapColNameToColTypeName(ctx, tx, table.Name)
	if err != nil {
		return xerrors.Errorf("unable to get column types: %w", err)
	}

	rows, err := tx.QueryContext(ctx, querySelect)
	if err != nil {
		logger.Log.Error("rows select error", log.Error(err))
		return xerrors.Errorf("Unable to select data from table %v: %w", table.Fqtn(), err)
	}
	defer rows.Close()

	err = readRowsAndPushByChunks(
		s.ConnectionParams.Location,
		rows,
		st,
		table,
		currTableSchema,
		colNameToColTypeName,
		chunkSize,
		pos.LSN,
		s.IsHomo,
		pusher,
	)
	if err != nil {
		return xerrors.Errorf("unable to read rows and push by chunks: %w", err)
	}

	err = rows.Err()
	if err != nil {
		msg := "Unable to read rows"
		logger.Log.Warn(msg, log.Error(err))
		return xerrors.Errorf("%v: %w", msg, err)
	}
	logger.Log.Info("Done read rows")
	return nil
}
