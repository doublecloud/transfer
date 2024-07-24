package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/doublecloud/tross/library/go/core/log"
	"github.com/doublecloud/tross/library/go/core/metrics"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/format"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/stats"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/util"
	"github.com/dustin/go-humanize"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

const (
	slotByteLagQuery   = `select pg_current_wal_lsn() - restart_lsn as size from pg_replication_slots where slot_name = $1`
	slotByteLagQuery96 = `select pg_current_xlog_location() - restart_lsn as size from pg_replication_slots where slot_name = $1`
	slotExistsQuery    = `SELECT EXISTS(SELECT * FROM pg_replication_slots WHERE slot_name = $1 AND (database IS NULL OR database = $2))`
	peekFromSlotQuery  = `SELECT pg_logical_slot_peek_changes($1, NULL, 1)`

	healthCheckInterval = 5 * time.Second
)

type SlotMonitor struct {
	conn   *pgxpool.Pool
	stopCh chan struct{}

	slotName         string
	slotDatabaseName string

	metrics *stats.SourceStats

	logger log.Logger
}

// NewSlotMonitor constructs a slot monitor, but does NOT start it.
//
// The provided pool is NOT closed by the monitor. It is the caller's responsibility to manage the pool.
func NewSlotMonitor(conn *pgxpool.Pool, slotName string, slotDatabaseName string, metrics *stats.SourceStats, logger log.Logger) *SlotMonitor {
	return &SlotMonitor{
		conn:   conn,
		stopCh: make(chan struct{}),

		slotName:         slotName,
		slotDatabaseName: slotDatabaseName,

		metrics: metrics,
		logger:  logger,
	}
}

func (m *SlotMonitor) Close() {
	if util.IsOpen(m.stopCh) {
		close(m.stopCh)
	}
}

func (m *SlotMonitor) describeError(err error) error {
	if err == pgx.ErrNoRows {
		return xerrors.Errorf("replication slot %s does not exist", m.slotName)
	}
	return err
}

func bytesToString(bytes int64) string {
	if bytes >= 0 {
		return humanize.Bytes(uint64(bytes))
	}
	return fmt.Sprintf("-%v", humanize.Bytes(uint64(-bytes)))
}

func (m *SlotMonitor) StartSlotMonitoring(maxSlotByteLag int64) <-chan error {
	result := make(chan error, 1)

	version := ResolveVersion(m.conn)

	go func() {
		defer close(result)
		ticker := time.NewTicker(healthCheckInterval)
		defer ticker.Stop()
		for {
			select {
			case <-m.stopCh:
				return
			case <-ticker.C:
				if err := m.checkSlot(version, maxSlotByteLag); err != nil {
					m.logger.Warn("check slot return error", log.Error(err))
					result <- err
					return
				}
			}
		}
	}()

	return result
}

func (m *SlotMonitor) checkSlot(version PgVersion, maxSlotByteLag int64) error {
	slotExists, err := m.slotExists(context.TODO())
	if err != nil {
		return xerrors.Errorf("unable to check existence of the slot %q: %w", m.slotName, err)
	}
	if !slotExists {
		return abstract.NewFatalError(xerrors.Errorf("slot %q has disappeared", m.slotName))
	}

	if err := m.validateSlot(context.TODO()); err != nil {
		return xerrors.Errorf("slot %q has become invalid: %w", m.slotName, err)
	}

	var slotByteLag int64
	var slotByteLagErr error
	if version.Is9x {
		slotByteLag, slotByteLagErr = m.getLag(slotByteLagQuery96)
	} else {
		slotByteLag, slotByteLagErr = m.getLag(slotByteLagQuery)
	}
	if slotByteLagErr != nil {
		return xerrors.Errorf("failed to check replication slot lag: %w", m.describeError(err))
	}
	m.logger.Infof("replication slot %q WAL lag %s / %s", m.slotName, bytesToString(slotByteLag), format.SizeUInt64(uint64(maxSlotByteLag)))
	m.metrics.Usage.Set(float64(slotByteLag))
	if slotByteLag > maxSlotByteLag {
		return abstract.NewFatalError(xerrors.Errorf("byte lag for replication slot %q exceeds the limit: %d > %d", m.slotName, slotByteLag, maxSlotByteLag))
	}

	return nil
}

func (m *SlotMonitor) slotExists(ctx context.Context) (bool, error) {
	var result bool
	checkSlot := func() error {
		err := m.conn.QueryRow(ctx, slotExistsQuery, m.slotName, m.slotDatabaseName).Scan(&result)
		if err != nil {
			if !util.IsOpen(m.stopCh) {
				return nil
			}
			return xerrors.Errorf("failed to check slot existence: %w", err)
		}
		return nil
	}
	err := backoff.Retry(checkSlot, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 5))
	if err != nil {
		return false, err
	}
	return result, nil
}

func (m *SlotMonitor) validateSlot(ctx context.Context) error {
	return nil
	// Disabled for now: https://st.yandex-team.ru/TM-4783
	/*
		validateSlot := func() error {
			rows, err := m.conn.Query(ctx, peekFromSlotQuery, m.slotName)
			if err != nil {
				if !util.IsOpen(m.stopCh) {
					return nil
				}
				// We need to check error code here and return FatalError only when the slot is invalid
				if IsPgError(err, ErrcObjectNotInPrerequisiteState) {
					return abstract.NewFatalError(xerrors.Errorf("replication slot %q is no longer readable: %w", m.slotName, err))
				}
				return xerrors.Errorf("slot validity check failed: %w", err)
			}
			rows.Close()
			return nil
		}
		if err := backoff.Retry(validateSlot, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 5)); err != nil {
			return err
		}
		return nil
	*/
}

func (m *SlotMonitor) getLag(monitorQ string) (int64, error) {
	var slotByteLag int64
	getByteLag := func() error {
		err := m.conn.QueryRow(context.TODO(), monitorQ, m.slotName).Scan(&slotByteLag)
		if err != nil && !util.IsOpen(m.stopCh) {
			return nil
		}
		return err
	}
	err := backoff.Retry(getByteLag, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 5))
	return slotByteLag, err
}

type PostgresSlotKiller struct {
	Slot AbstractSlot
}

func (k *PostgresSlotKiller) KillSlot() error {
	return k.Slot.Suicide()
}

func RunSlotMonitor(ctx context.Context, pgSrc *PgSource, registry metrics.Registry) (abstract.SlotKiller, <-chan error, error) {
	rb := util.Rollbacks{}
	defer rb.Do()

	connPool, err := MakeConnPoolFromSrc(pgSrc, logger.Log)
	if err != nil {
		return nil, nil, xerrors.Errorf("unable to make source connection pool: %w", err)
	}
	rb.Add(func() {
		connPool.Close()
	})

	slotMonitor := NewSlotMonitor(connPool, pgSrc.SlotID, pgSrc.Database, stats.NewSourceStats(registry), logger.Log)
	rb.Add(func() {
		slotMonitor.Close()
	})

	go func() {
		<-ctx.Done()
		slotMonitor.Close()
		logger.Log.Info("slot monitor is closed by context")
	}()

	exists, err := slotMonitor.slotExists(context.TODO())
	if err != nil {
		return nil, nil, xerrors.Errorf("unable to check existence of the slot: %w", err)
	}
	if !exists {
		return abstract.MakeStubSlotKiller(), nil, nil
	}

	errChan := slotMonitor.StartSlotMonitoring(int64(pgSrc.SlotByteLagLimit))

	slot, err := NewSlot(slotMonitor.conn, logger.Log, pgSrc)
	if err != nil {
		return nil, nil, xerrors.Errorf("unable to create new slot: %w", err)
	}

	rb.Cancel()
	return &PostgresSlotKiller{Slot: slot}, errChan, nil
}
