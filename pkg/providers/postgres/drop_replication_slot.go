package postgres

import (
	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
)

func DropReplicationSlot(src *PgSource) error {
	conn, err := MakeConnPoolFromSrc(src, logger.Log)
	if err != nil {
		return xerrors.Errorf("failed to create a connection pool: %w", err)
	}
	defer conn.Close()

	slot, err := NewSlot(conn, logger.Log, src)
	if err != nil {
		return xerrors.Errorf("failed to create a replication slot object: %w", err)
	}
	defer slot.Close()

	err = slot.Suicide()
	if err != nil {
		return xerrors.Errorf("failed to drop the replication slot: %w", err)
	}

	return nil
}
