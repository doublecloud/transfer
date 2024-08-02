package snapshot

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/doublecloud/tross/library/go/core/log"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/base"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/middlewares"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/oracle"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/oracle/common"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/oracle/schema"
	"github.com/jmoiron/sqlx"
)

// loader is the basic provider of snapshot table load for Oracle source
type loader struct {
	sqlxDB   *sqlx.DB
	config   *oracle.OracleSource
	table    *schema.Table
	position *common.LogPosition

	current uint64

	logger log.Logger
}

func newLoader(sqlxDB *sqlx.DB, config *oracle.OracleSource, position *common.LogPosition, table *schema.Table, logger log.Logger) *loader {
	return &loader{
		sqlxDB:   sqlxDB,
		config:   config,
		table:    table,
		position: position,

		current: 0,

		logger: logger,
	}
}

// rowsInBatch determines the size of the batch sent into bufferer. It should be small enough to ensure the buffer is not too overflowed
const rowsInBatch int = 512

// LoadSnapshot implements snapshot load to the given target
func (l *loader) LoadSnapshot(ctx context.Context, syncTarget middlewares.Asynchronizer, sql string) error {
	rawValues, err := createRawValues(l.table)
	if err != nil {
		return xerrors.Errorf("Can't create raw values for table '%v': %w", l.table.OracleSQLName(), err)
	}

	batch := []base.Event{}
	batchTime := time.Now()
	queryErr := common.PDBQueryGlobal(
		l.config,
		l.sqlxDB,
		ctx,
		func(ctx context.Context, connection *sqlx.Conn) error {
			rows, err := connection.QueryContext(ctx, sql)
			if err != nil {
				return xerrors.Errorf("Can't select table '%v': %w", l.table.OracleSQLName(), err)
			}
			defer rows.Close()

			for rows.Next() {
				if err := rows.Scan(rawValues...); err != nil {
					return xerrors.Errorf("Can't scan values for table '%v': %w", l.table.OracleSQLName(), err)
				}

				event, err := createInsertEvent(l.table, rawValues, l.position)
				if err != nil {
					return xerrors.Errorf("Can't parse raw values for table '%v': %w", l.table.OracleSQLName(), err)
				}
				batch = append(batch, event)

				if len(batch) >= rowsInBatch {
					if err := l.pushBatch(batch, &batchTime, syncTarget); err != nil {
						return xerrors.Errorf("failed to push to target: %w", err)
					}
					batch = []base.Event{}
				}
			}
			if rows.Err() != nil {
				return xerrors.Errorf("Can't read row from DB: %w", rows.Err())
			}

			return nil
		},
	)
	if queryErr != nil {
		return xerrors.Errorf("failed while executing query '%s' in Oracle: %w", sql, queryErr)
	}

	if len(batch) > 0 {
		if err := l.pushBatch(batch, &batchTime, syncTarget); err != nil {
			return xerrors.Errorf("failed to push to target: %w", err)
		}
		batch = []base.Event{}
	}

	return nil
}

func (l *loader) pushBatch(batch []base.Event, batchTime *time.Time, syncTarget middlewares.Asynchronizer) error {
	if err := syncTarget.Push(base.NewEventBatch(batch)); err != nil {
		return xerrors.Errorf("failed to push a batch of %d events: %w", len(batch), err)
	}
	atomic.AddUint64(&l.current, uint64(len(batch)))
	*batchTime = time.Now()
	return nil
}

func (l *loader) Current() uint64 {
	return atomic.LoadUint64(&l.current)
}
