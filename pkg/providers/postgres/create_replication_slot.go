package postgres

import (
	"github.com/cenkalti/backoff/v4"
	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/util"
)

func CreateReplicationSlot(src *PgSource) error {
	if err := backoff.RetryNotify(func() error {
		defer func() {
			if r := recover(); r != nil {
				logger.Log.Warnf("Recovered from panic while creating a replication slot:\n%v", r)
			}
		}()
		conn, err := MakeConnPoolFromSrc(src, logger.Log)
		if err != nil {
			return xerrors.Errorf("failed to create a connection pool: %w", err)
		}
		defer conn.Close()
		slot, err := NewSlot(conn, logger.Log, src)
		if err != nil {
			return xerrors.Errorf("failed to create a replication slot object: %w", err)
		}

		exist, err := slot.Exist()
		if err != nil {
			return xerrors.Errorf("failed to check existence of a replication slot: %w", err)
		}
		if exist {
			logger.Log.Infof("replication slot already exists, try to drop it")
			if err := slot.Suicide(); err != nil {
				return xerrors.Errorf("failed to drop the replication slot: %w", err)
			}
			return xerrors.New("a replication slot already exists")
		}

		if err := slot.Create(); err != nil {
			return xerrors.Errorf("failed to create a replication slot: %w", err)
		}

		logger.Log.Info("Replication slot created, re-check existence")
		exist, err = slot.Exist()
		if err != nil {
			return xerrors.Errorf("failed to check existence of a replication slot: %w", err)
		}
		if !exist {
			return xerrors.New("replication slot was created, but the check shows it does not exist")
		}
		return nil
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3), util.BackoffLogger(logger.Log, "create replication slot")); err != nil {
		return xerrors.Errorf("failed to create a replication slot: %w", err)
	}

	return nil
}
