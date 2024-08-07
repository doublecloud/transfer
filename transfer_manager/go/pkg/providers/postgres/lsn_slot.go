package postgres

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/util"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"go.ytsaurus.tech/library/go/core/log"
)

var (
	InitLSNSlotDDL = fmt.Sprintf(`
create table if not exists %%s.%s (
	slot_id   text primary key,
	iterator int,
	replicated_iterator int,
	last_iterator_update timestamp,
	last_replicated_update timestamp,
	committed_lsn text
);`, TableMoleFinder)

	MoveIteratorQuery = fmt.Sprintf(`
update %%s.%s set iterator = iterator + 1, last_iterator_update = now()
where slot_id = $1;`, TableMoleFinder)

	UpdateReplicatedQuery = fmt.Sprintf(`
update %%s.%s set replicated_iterator = $2, last_replicated_update = now()
where slot_id = $1 and replicated_iterator = $2 - 1;`, TableMoleFinder)

	SelectCommittedLSN = fmt.Sprintf(`
select committed_lsn from %%s.%s where slot_id = $1;`, TableMoleFinder)

	InsertIntoMoleFinder = fmt.Sprintf(`
insert into %%s.%s
	(slot_id, iterator, replicated_iterator, last_iterator_update, last_replicated_update, committed_lsn)
values
	($1, null, null, now(), now(), $2)
on conflict (slot_id) do update
	set
		replicated_iterator = null,
		iterator = null,
		last_iterator_update = now(),
		last_replicated_update = now(),
		committed_lsn = excluded.committed_lsn;`, TableMoleFinder)

	DeleteFromMoleFinder = fmt.Sprintf(`delete from %%s.%s where slot_id = $1`, TableMoleFinder)

	InsertIntoMoleFinder2 = fmt.Sprintf(`
insert into %%s.%s (slot_id, committed_lsn)
values ($1, $2)
on conflict (slot_id) do update set committed_lsn = excluded.committed_lsn;`, TableMoleFinder)

	SelectCommittedLSN2 = fmt.Sprintf(`
select committed_lsn, replicated_iterator from %%s.%s where slot_id = $1`, TableMoleFinder)

	SelectLsnForSlot = `select restart_lsn from pg_replication_slots where slot_name = $1;`
)

func GetInitLSNSlotDDL(schema string) string {
	return fmt.Sprintf(InitLSNSlotDDL, schema)
}

func GetMoveIteratorQuery(schema string) string {
	return fmt.Sprintf(MoveIteratorQuery, schema)
}

func GetUpdateReplicatedQuery(schema string) string {
	return fmt.Sprintf(UpdateReplicatedQuery, schema)
}

func GetSelectCommittedLSN(schema string) string {
	return fmt.Sprintf(SelectCommittedLSN, schema)
}

func GetInsertIntoMoleFinder(schema string) string {
	return fmt.Sprintf(InsertIntoMoleFinder, schema)
}

func GetDeleteFromMoleFinder(schema string) string {
	return fmt.Sprintf(DeleteFromMoleFinder, schema)
}

func GetInsertIntoMoleFinder2(schema string) string {
	return fmt.Sprintf(InsertIntoMoleFinder2, schema)
}

func GetSelectCommittedLSN2(schema string) string {
	return fmt.Sprintf(SelectCommittedLSN2, schema)
}

type LsnTrackedSlot struct {
	logger    log.Logger
	once      sync.Once
	slotID    string
	childSlot AbstractSlot
	src       *PgSource
	schema    string
	stopCh    chan error
	Conn      *pgxpool.Pool
	lastMove  time.Time
}

func (l *LsnTrackedSlot) Init(sink abstract.AsyncSink) error {
	return <-sink.AsyncPush([]abstract.ChangeItem{{
		CommitTime:   uint64(time.Now().UnixNano()),
		Kind:         "pg:DDL", // TODO: Replace with generic kind
		ColumnValues: []interface{}{GetInitLSNSlotDDL(l.schema)},
	}})
}

func (l *LsnTrackedSlot) Exist() (bool, error) {
	if _, err := l.Conn.Exec(context.TODO(), GetInitLSNSlotDDL(l.schema)); err != nil {
		l.logger.Errorf("failed create table: %v", err)
		return false, xerrors.Errorf("failed create lsn track table: %w", err)
	}
	row := l.Conn.QueryRow(context.TODO(), fmt.Sprintf(`
select exists(
   select *
     from pg_replication_slots
   where slot_name = '%v' and (database is null or database = '%v')
);`, l.slotID, l.src.Database))

	var exist bool
	if err := row.Scan(&exist); err != nil {
		l.logger.Errorf("failed execute exists query: %v", err)
		return false, xerrors.Errorf("failed execute exists query: %w", err)
	}
	if !exist {
		var lsn string
		row := l.Conn.QueryRow(context.TODO(), GetSelectCommittedLSN(l.schema), l.slotID)

		if err := row.Scan(&lsn); err != nil {
			if err == pgx.ErrNoRows {
				return false, nil
			}
			l.logger.Errorf("failed select lsn from lsn track table: %v", err)
			return false, xerrors.Errorf("failed select lsn from lsn track table: %w", err)
		}
		if lsn == "" {
			return false, nil
		}
		if err := l.createFromLSN(lsn); err != nil {
			l.logger.Errorf("failed create slot from saved lsn: %v", err)
			return false, xerrors.Errorf("failed create slot from saved lsn: %w", err)
		}
		return true, nil
	}
	return exist, nil
}

func (l *LsnTrackedSlot) Create() error {
	rb := util.Rollbacks{}
	if err := l.childSlot.Create(); err != nil {
		return xerrors.Errorf("unable to create child slot: %w", err)
	}
	defer rb.Do()
	rb.Add(func() {
		if err := l.childSlot.Suicide(); err != nil {
			l.logger.Error("unable to kill child slot", log.Error(err))
		}
	})
	var lsn string
	if err := l.Conn.QueryRow(context.TODO(), SelectLsnForSlot, l.slotID).Scan(&lsn); err != nil {
		return err
	}
	l.logger.Infof("Inferred slot: %v | lsn: %v", l.slotID, lsn)
	if _, err := l.Conn.Exec(context.TODO(), GetInsertIntoMoleFinder(l.schema), l.slotID, lsn); err != nil {
		return xerrors.Errorf("unable to insert into mole finder: %w", err)
	}
	rb.Cancel()
	return nil
}

func (l *LsnTrackedSlot) Suicide() error {
	if _, err := l.Conn.Exec(context.TODO(), GetDeleteFromMoleFinder(l.schema), l.slotID); err != nil {
		l.logger.Warnf("failed cleanup %s table: %v", TableMoleFinder, err)
	}
	return l.childSlot.Suicide()
}

func (l *LsnTrackedSlot) createFromLSN(lsn string) error {
	return backoff.Retry(func() error {
		rb := util.Rollbacks{}
		defer rb.Do()

		conn, err := MakeConnPoolFromSrc(l.src, l.logger)
		if err != nil {
			return xerrors.Errorf("could not create slot from lsn:%v because of error: %w", lsn, err)
		}
		rb.Add(func() {
			conn.Close()
		})

		l.Conn.Close()
		l.Conn = conn
		_, err = l.Conn.Exec(context.TODO(), fmt.Sprintf(`
BEGIN;
SET LOCAL lock_timeout = '0';
select * from pg_create_logical_replication_slot_lsn('%v', 'wal2json', false, pg_lsn('%v'));
COMMIT;
`, l.slotID, lsn))

		if err != nil {
			return xerrors.Errorf("could not create slot from lsn:%v because of error: %w", lsn, err)
		}
		l.logger.Infof("slot created from lsn:%v", lsn)
		rb.Cancel()
		return nil
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 10))
}

func (l *LsnTrackedSlot) Move(lsn string) error {
	if time.Since(l.lastMove).Seconds() <= 1 {
		l.logger.Debugf("Skip move for lsn: %v, to fast moves: %v", lsn, time.Since(l.lastMove))
		return nil
	}
	if _, err := l.Conn.Exec(context.TODO(), GetInsertIntoMoleFinder2(l.schema), l.slotID, lsn); err != nil {
		return err
	}
	l.lastMove = time.Now()
	return nil
}

func (l *LsnTrackedSlot) Close() {
	l.once.Do(func() {
		close(l.stopCh)
		l.Conn.Close()
	})
}

func (l *LsnTrackedSlot) Run() {
	var conn *pgxpool.Pool
	for {
		time.Sleep(time.Second * 5)

		select {
		case <-l.stopCh:
			if conn != nil {
				conn.Close()
			}
			return
		default:
		}

		if conn == nil {
			var err error
			conn, err = MakeConnPoolFromSrc(l.src, l.logger)
			if err != nil {
				// if catch error, try to reconnect next time
				conn = nil
				l.logger.Warn("Unable to init pg conn", log.Error(err))
				continue
			}
		}

		if _, err := conn.Exec(context.TODO(), GetMoveIteratorQuery(l.schema), l.slotID); err != nil {
			// if catch error, close connection and try to reconnect next time
			conn.Close()
			conn = nil
			l.logger.Warn("Unable to move iterator query", log.Error(err))
		}
	}
}

func (l *LsnTrackedSlot) CheckMonotonic(ci abstract.ChangeItem) (bool, error) {
	if ci.Table != TableMoleFinder {
		return false, nil
	}
	if ci.Kind != abstract.UpdateKind {
		return true, nil
	}
	if ci.ColumnValues[0].(string) != l.slotID {
		return true, nil
	}
	// if iterator column did not change ignore
	if ci.ColumnValues[1] == ci.OldKeys.KeyValues[1] {
		return true, nil
	}
	// iterator already in sync
	if ci.ColumnValues[1] == ci.ColumnValues[2] {
		return true, nil
	}
	l.logger.Infof("iterator advanced, need to increment replicated iterator: %v -> %v", ci.OldKeys.KeyValues[1], ci.ColumnValues[1])
	return true, l.UpdateReplicated(ci.ColumnValues[1].(int), ci.LSN)
}

func (l *LsnTrackedSlot) UpdateReplicated(iter int, lsn uint64) error {
	if stmt, err := l.Conn.Exec(context.TODO(), GetUpdateReplicatedQuery(l.schema), l.slotID, iter); err != nil {
		return xerrors.Errorf("unable to execute update replicated query: %w", err)
	} else if stmt.RowsAffected() != 1 {
		var lastLSN string
		var curIter int
		if err := l.Conn.QueryRow(context.TODO(), GetSelectCommittedLSN2(l.schema), l.slotID).Scan(&lastLSN, &curIter); err != nil {
			return xerrors.Errorf("unable to select committed lsn: %w", err)
		}
		committedLSN, err := pglogrepl.ParseLSN(lastLSN)
		if err != nil {
			return xerrors.Errorf("unable to parse lsn: %w", err)
		}
		if lsn < uint64(committedLSN) {
			l.logger.Warnf("restart tail, should not be fatal: %v -> %v", lsn, lastLSN)
			return nil
		}
		if curIter >= iter {
			l.logger.Warnf("same iter update: %v -> %v, iter: %v", lsn, lastLSN, curIter)
			return nil
		}
		return abstract.NewFatalError(xerrors.Errorf("mismatch in mole finder, found a hole. Query not match LSN: %v -> %v, Iterator: %v -> %v", lsn, lastLSN, curIter, iter))
	} else {
		l.logger.Debugf("updated rows: %v", stmt.RowsAffected())
	}
	return nil
}

func NewLsnTrackedSlot(pool *pgxpool.Pool, logger log.Logger, src *PgSource) AbstractSlot {
	return &LsnTrackedSlot{
		logger:    logger,
		once:      sync.Once{},
		slotID:    src.SlotID,
		childSlot: NewNotTrackedSlot(pool, logger, src),
		src:       src,
		schema:    src.KeeperSchema,
		stopCh:    make(chan error),
		Conn:      pool,
		lastMove:  time.Time{},
	}
}
