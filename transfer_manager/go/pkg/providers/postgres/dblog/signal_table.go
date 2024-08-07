package dblog

import (
	"context"
	"fmt"

	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/dblog"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/util"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"go.ytsaurus.tech/library/go/core/log"
)

type (
	txOp func(conn pgx.Tx) error
)

const (
	signalTableName        = "__data_transfer_signal_table"
	tableSchemaColumnIndex = 0
	tableNameColumnIndex   = 1
	tableTransferIDIndex   = 2
	markTypeColumnIndex    = 4
)

type signalTable struct {
	conn       *pgxpool.Pool
	logger     log.Logger
	transferID string
}

func buildSignalTableDDL() string {
	query := `CREATE TABLE IF NOT EXISTS %s
			  (
				  table_schema TEXT,
				  table_name TEXT,
				  transfer_id TEXT,
				  mark UUID,
				  markType CHAR,
				  PRIMARY KEY (table_schema, table_name, transfer_id)
			  );`

	return fmt.Sprintf(query, signalTableName)
}

func newPgSignalTable(
	ctx context.Context,
	conn *pgxpool.Pool,
	logger log.Logger,
	transferID string,
) (*signalTable, error) {

	pgSignalTable := &signalTable{
		conn:       conn,
		logger:     logger,
		transferID: transferID,
	}

	if err := pgSignalTable.init(ctx); err != nil {
		return nil, xerrors.Errorf("unable to initialize signal table: %w", err)
	}

	return pgSignalTable, nil
}

func (s *signalTable) init(ctx context.Context) error {
	return s.tx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, buildSignalTableDDL()); err != nil {
			return xerrors.Errorf("failed to ensure existence of the signal table service table: %w", err)
		}
		return nil
	})
}

func (s *signalTable) tx(ctx context.Context, operation txOp) error {
	tx, err := s.conn.Begin(ctx)
	if err != nil {
		return xerrors.Errorf("unable to begin transaction: %w", err)
	}

	if err := operation(tx); err != nil {
		if err := tx.Rollback(ctx); err != nil {
			s.logger.Warn("Unable to rollback", log.Error(err))
		}
		return xerrors.Errorf("unable to execute operation: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return xerrors.Errorf("unable to commit transaction: %w", err)
	}

	return nil
}

func (s *signalTable) CreateWatermark(ctx context.Context, tableID abstract.TableID, watermarkType dblog.WatermarkType) error {
	return s.tx(ctx, func(conn pgx.Tx) error {
		tx, err := s.conn.Begin(ctx)
		if err != nil {
			return xerrors.Errorf("unable to begin transaction: %w", err)
		}

		rollback := util.Rollbacks{}
		defer rollback.Do()
		rollback.Add(func() {
			if err := tx.Rollback(ctx); err != nil {
				s.logger.Info("Unable to rollback", log.Error(err))
			}
		})

		newUUID := uuid.New()

		_, err = tx.Exec(
			ctx,
			s.makeWatermarkQuery(),
			tableID.Namespace,
			tableID.Name,
			s.transferID,
			newUUID,
			string(watermarkType),
		)
		if err != nil {
			return xerrors.Errorf("failed to create watermark for %s: %w", tableID.Fqtn(), err)
		}

		if err := tx.Commit(ctx); err != nil {
			return xerrors.Errorf("unable to commit transaction: %w", err)
		}

		rollback.Cancel()
		return nil
	})
}

func (s *signalTable) IsWatermark(item *abstract.ChangeItem, tableID abstract.TableID) (bool, dblog.WatermarkType) {
	isWatermark := item.Table == signalTableName
	if !isWatermark {
		return false, dblog.LowWatermarkType
	}

	if item.ColumnValues[tableSchemaColumnIndex].(string) != tableID.Namespace ||
		item.ColumnValues[tableNameColumnIndex].(string) != tableID.Name ||
		item.ColumnValues[tableTransferIDIndex].(string) != s.transferID {
		return false, dblog.LowWatermarkType
	}

	if _, ok := item.ColumnValues[markTypeColumnIndex].(string); !ok {
		return false, dblog.LowWatermarkType
	}
	strVal := item.ColumnValues[markTypeColumnIndex].(string)
	if len(strVal) != 1 {
		return false, dblog.LowWatermarkType
	}

	return true, dblog.WatermarkType(strVal)
}

func (s *signalTable) makeWatermarkQuery() string {
	query := `INSERT INTO %s (table_schema, table_name, transfer_id, mark, markType)
			  VALUES (($1), ($2), ($3), ($4), ($5))
			  ON CONFLICT (table_schema, table_name, transfer_id)
			  DO UPDATE
			  SET mark = EXCLUDED.mark,
				  markType = EXCLUDED.markType;`

	return fmt.Sprintf(query, signalTableName)
}
