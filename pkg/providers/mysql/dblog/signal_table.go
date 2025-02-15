package dblog

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/dblog"
	"github.com/doublecloud/transfer/pkg/util"
	"github.com/google/uuid"
	"go.ytsaurus.tech/library/go/core/log"
)

type (
	txOp func(conn *sql.Tx) error
)

const (
	SignalTableName = "__data_transfer_signal_table"
)

type signalTable struct {
	logger     log.Logger
	transferID string
	schemaName string
	db         *sql.DB
}

func buildSignalTableDDL(schemaName string) string {
	query := `CREATE TABLE IF NOT EXISTS %s.%s
			  (
				  table_schema VARCHAR(255),
				  table_name VARCHAR(255),
				  transfer_id VARCHAR(255),
				  mark CHAR(36),
				  mark_type CHAR(1),
				  low_bound TEXT,
				  PRIMARY KEY (table_schema, table_name, transfer_id, mark_type)
			  );`

	return fmt.Sprintf(query, schemaName, SignalTableName)
}

func (s *signalTable) makeWatermarkQuery() string {
	query := `INSERT INTO %s.%s (table_schema, table_name, transfer_id, mark, mark_type, low_bound)
			  VALUES (?, ?, ?, ?, ?, ?)
			  ON DUPLICATE KEY UPDATE
			  mark = VALUES(mark),
			  low_bound = VALUES(low_bound);`

	return fmt.Sprintf(query, s.schemaName, SignalTableName)
}

func (s *signalTable) resolveLowBoundQuery() string {
	query := `SELECT low_bound FROM %s.%s
	          WHERE table_schema = ?
	          AND table_name = ?
	          AND transfer_id = ?
	          AND mark_type = ?;`

	return fmt.Sprintf(query, s.schemaName, SignalTableName)
}

func deleteWatermarksQuery(schemaName string) string {
	query := `DELETE FROM %s.%s WHERE transfer_id = ?;`
	return fmt.Sprintf(query, schemaName, SignalTableName)
}

func signalTableExist(ctx context.Context, conn *sql.DB, schemaName string) (bool, error) {
	query := `SELECT EXISTS (
		SELECT 1 
		FROM information_schema.tables 
		WHERE table_schema = ? 
		AND table_name = ?
	   );`

	var exist bool
	if err := conn.QueryRowContext(ctx, query, schemaName, SignalTableName).Scan(&exist); err != nil {
		return false, err
	}

	return exist, nil
}

func NewSignalTable(
	ctx context.Context,
	db *sql.DB,
	logger log.Logger,
	transferID string,
	schemaName string,
) (*signalTable, error) {
	tbl := &signalTable{
		db:         db,
		logger:     logger,
		transferID: transferID,
		schemaName: schemaName,
	}

	if err := tbl.init(ctx); err != nil {
		return nil, xerrors.Errorf("unable to initialize signal table: %w", err)
	}

	return tbl, nil
}

func (s *signalTable) init(ctx context.Context) error {
	return s.tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, buildSignalTableDDL(s.schemaName)); err != nil {
			return xerrors.Errorf("failed to ensure existence of the signal table service table: %w", err)
		}
		return nil
	})
}

func (s *signalTable) tx(ctx context.Context, operation txOp) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return xerrors.Errorf("unable to begin transaction: %w", err)
	}

	if err := operation(tx); err != nil {
		if err := tx.Rollback(); err != nil {
			s.logger.Warn("Unable to rollback", log.Error(err))
		}
		return xerrors.Errorf("unable to execute operation: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return xerrors.Errorf("unable to commit transaction: %w", err)
	}

	return nil
}

func (s *signalTable) CreateWatermark(
	ctx context.Context,
	tableID abstract.TableID,
	watermarkType dblog.WatermarkType,
	lowBoundArr []string,
) (uuid.UUID, error) {
	var newUUID uuid.UUID
	err := s.tx(ctx, func(_ *sql.Tx) error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return xerrors.Errorf("unable to begin transaction: %w", err)
		}

		rollback := util.Rollbacks{}
		defer rollback.Do()
		rollback.Add(func() {
			if err := tx.Rollback(); err != nil {
				s.logger.Info("Unable to rollback", log.Error(err))
			}
		})

		newUUID = uuid.New()

		lowBoundStr, err := dblog.ConvertArrayToString(lowBoundArr)
		if err != nil {
			return xerrors.Errorf("unable to convert low bound array to string")
		}

		query := s.makeWatermarkQuery()
		s.logger.Info(
			fmt.Sprintf("CreateWatermark - query: %s", strings.ReplaceAll(query, "\n", "")),
			log.String("tableID.Namespace", tableID.Namespace),
			log.String("tableID.Name", tableID.Name),
			log.String("s.transferID", s.transferID),
			log.String("newUUID", newUUID.String()),
			log.String("watermarkType", string(watermarkType)),
			log.String("lowBoundStr", lowBoundStr),
		)

		_, err = tx.ExecContext(
			ctx,
			query,
			tableID.Namespace,
			tableID.Name,
			s.transferID,
			newUUID,
			string(watermarkType),
			lowBoundStr,
		)
		if err != nil {
			return xerrors.Errorf("failed to create watermark for %s: %w", tableID.Fqtn(), err)
		}

		if err := tx.Commit(); err != nil {
			return xerrors.Errorf("unable to commit transaction: %w", err)
		}

		rollback.Cancel()
		return nil
	})

	return newUUID, err
}

func (s *signalTable) IsWatermark(item *abstract.ChangeItem, tableID abstract.TableID, markUUID uuid.UUID) (bool, dblog.WatermarkType) {
	isWatermark := item.Table == SignalTableName
	if !isWatermark {
		return false, dblog.BadWatermarkType
	}

	row := item.AsMap()
	if item.Kind == abstract.DeleteKind ||
		row["table_schema"].(string) != tableID.Namespace ||
		row["table_name"].(string) != tableID.Name ||
		row["transfer_id"].(string) != s.transferID {
		return false, dblog.BadWatermarkType
	}

	parsedUUID, err := uuid.Parse(row["mark"].(string))
	if err != nil {
		return true, dblog.BadWatermarkType
	}

	if parsedUUID != markUUID {
		return true, dblog.BadWatermarkType
	}

	if _, ok := row["mark_type"].(string); !ok {
		return false, dblog.BadWatermarkType
	}
	strVal := row["mark_type"].(string)
	if len(strVal) != 1 {
		return false, dblog.BadWatermarkType
	}

	return true, dblog.WatermarkType(strVal)
}

func (s *signalTable) resolveLowBound(ctx context.Context, tableID abstract.TableID) []string {
	var lowBoundStr string

	err := s.tx(ctx, func(conn *sql.Tx) error {
		query := s.resolveLowBoundQuery()

		err := conn.QueryRowContext(ctx, query, tableID.Namespace, tableID.Name, s.transferID, dblog.SuccessWatermarkType).Scan(&lowBoundStr)
		if err != nil {
			return xerrors.Errorf("failed to retrieve low_bound for Namespace: %s, Table: %s, transferID: %s, err : %w", tableID.Namespace, tableID.Name, s.transferID, err)
		}

		return nil
	})

	if err != nil {
		return nil
	}

	lowBoundArray, err := dblog.ConvertStringToArray(lowBoundStr)
	if err != nil {
		return nil
	}

	return lowBoundArray
}

func DeleteWatermarks(ctx context.Context, db *sql.DB, schemaName string, transferID string) error {
	signalTableExist, err := signalTableExist(ctx, db, schemaName)
	if err != nil {
		return xerrors.Errorf("signal table check query failed err: %w", err)
	}
	if signalTableExist {
		query := deleteWatermarksQuery(schemaName)
		_, err := db.ExecContext(ctx, query, transferID)
		if err != nil {
			return xerrors.Errorf("failed to delete watermarks err: %w", err)
		}
	}

	return err
}
