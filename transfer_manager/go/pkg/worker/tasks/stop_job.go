package tasks

import (
	"time"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/config/env"
)

var ErrNoActiveOperation = xerrors.NewSentinel("TM: missed operation id")

func StopJob(cp coordinator.Coordinator, transfer server.Transfer) error {
	if transfer.SnapshotOnly() {
		return nil
	}
	if err := stopRuntime(cp, transfer); err != nil {
		return xerrors.Errorf("unable to stop runtime hook: %w", err)
	}
	return nil
}

var stopRuntime = func(cp coordinator.Coordinator, transfer server.Transfer) error {
	if env.IsTest() {
		return nil
	}
	logger.Log.Infof("Wait to change apply")
	time.Sleep(10 * time.Second)
	return nil
}
