package tasks

import (
	"context"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/postgres"
)

func RemoveTables(ctx context.Context, cp coordinator.Coordinator, transfer server.Transfer, task server.TransferOperation, tables []string) error {
	active, err := GetLeftTerminalSrcEndpoints(cp, transfer)
	if err != nil {
		return nil
	}
	if len(active) == 0 {
		return xerrors.New("RemoveTable supports maximum one-lb-in-the-middle case")
	}
	isRunning := transfer.Status == server.Running
	if isRunning {
		if err := StopJob(cp, transfer); err != nil {
			return xerrors.Errorf("stop job: %w", err)
		}
	}
	for _, src := range active {
		switch src := src.(type) {
		case *postgres.PgSource:
			tableSet := make(map[string]bool)
			for _, table := range src.DBTables {
				tableSet[table] = true
			}
			for _, table := range tables {
				tableSet[table] = false
			}
			src.DBTables = make([]string, 0)
			for k, v := range tableSet {
				if v {
					src.DBTables = append(src.DBTables, k)
				}
			}
			c, err := cp.GetEndpoint(transfer.ID, true)
			if err != nil {
				return xerrors.Errorf("Cannot load source endpoint to update tables list changes: %w", err)
			}
			source, _ := c.(server.Source)
			updatedSrc, _ := source.(*postgres.PgSource)
			updatedSrc.DBTables = src.DBTables
			updatedSrc.ExcludedTables = src.ExcludedTables
			if err := cp.UpdateEndpoint(transfer.ID, c); err != nil {
				return xerrors.Errorf("Cannot store source endpoint with tables changes: %w", err)
			}
		}
	}
	if !isRunning {
		return nil
	}
	if err := StartJob(ctx, cp, transfer, &task); err != nil {
		return xerrors.Errorf("unable to start job: %w", err)
	}

	return nil
}
