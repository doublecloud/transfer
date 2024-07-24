package tasks

import (
	"time"

	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
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
	logger.Log.Infof("Wait to change apply")
	time.Sleep(10 * time.Second)
	return nil
}
