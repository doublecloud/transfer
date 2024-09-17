package tasks

import (
	"context"
	"time"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/config/env"
	"github.com/doublecloud/transfer/pkg/errors"
	"github.com/doublecloud/transfer/pkg/errors/categories"
)

func StartJob(ctx context.Context, cp coordinator.Coordinator, transfer server.Transfer, task *server.TransferOperation) error {
	if !transfer.IsMain() {
		return nil
	}
	transfer.Status = server.Running
	if transfer.SnapshotOnly() {
		transfer.Status = server.Completed
	}
	if err := cp.SetStatus(transfer.ID, transfer.Status); err != nil {
		return errors.CategorizedErrorf(categories.Internal, "Cannot transit transfer into the %s state: %w", string(transfer.Status), err)
	}
	if transfer.SnapshotOnly() {
		return nil
	}

	if err := startRuntime(ctx, cp, transfer, task); err != nil {
		return xerrors.Errorf("unable to prepare runtime hook: %w", err)
	}
	if env.IsTest() {
		return nil
	}

	logger.Log.Info("Wait for transfer status change to apply")
	time.Sleep(10 * time.Second)
	logger.Log.Info("Transfer status change is considered completed")
	time.Sleep(time.Second)
	return nil
}

var startRuntime = func(ctx context.Context, cp coordinator.Coordinator, transfer server.Transfer, task *server.TransferOperation) error {
	return nil
}
