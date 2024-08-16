package tasks

import (
	"context"

	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/cleanup"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers"
	"go.ytsaurus.tech/library/go/core/log"
)

func CleanupResource(ctx context.Context, task server.TransferOperation, transfer server.Transfer, logger log.Logger, cp coordinator.Coordinator) error {
	err := cleanupTmp(ctx, transfer, logger, cp, task)
	if err != nil {
		return xerrors.Errorf("unable to cleanup tmp: %w", err)
	}

	if transfer.SnapshotOnly() {
		return nil
	}

	cleanuper, ok := providers.Source[providers.Cleanuper](logger, solomon.NewRegistry(solomon.NewRegistryOpts()), cp, &transfer)
	if !ok {
		logger.Infof("CleanupResource(%v) for transfer(%v) has no active resource", task.OperationID, transfer.ID)
		return nil
	}
	return cleanuper.Cleanup(ctx, &task)
}

func cleanupTmp(ctx context.Context, transfer server.Transfer, logger log.Logger, cp coordinator.Coordinator, task server.TransferOperation) error {
	tmpPolicy := transfer.TmpPolicy
	if tmpPolicy == nil {
		logger.Info("tmp policy is not set")
		return nil
	}

	err := server.EnsureTmpPolicySupported(transfer.Dst, &transfer)
	if err != nil {
		return xerrors.Errorf(server.ErrInvalidTmpPolicy, err)
	}

	cleanuper, ok := providers.Destination[providers.TMPCleaner](logger, solomon.NewRegistry(solomon.NewRegistryOpts()), cp, &transfer)
	if !ok {
		return nil
	}

	tmpCleaner, err := cleanuper.TMPCleaner(ctx, &task)
	if err != nil {
		return xerrors.Errorf("unable to initialize tmp cleaner: %w", err)
	}
	defer cleanup.Close(tmpCleaner, logger)

	err = tmpCleaner.CleanupTmp(ctx, transfer.ID, tmpPolicy)
	if err == nil {
		logger.Info("successfully cleaned up tmp")
	}
	return err
}
