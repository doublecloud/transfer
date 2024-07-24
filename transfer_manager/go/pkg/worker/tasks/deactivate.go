package tasks

import (
	"context"

	"github.com/doublecloud/tross/library/go/core/metrics"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers"
)

func Deactivate(ctx context.Context, cp coordinator.Coordinator, transfer server.Transfer, task server.TransferOperation, registry metrics.Registry) error {
	deactivator, ok := providers.Source[providers.Deactivator](logger.Log, registry, cp, &transfer)
	if ok {
		return deactivator.Deactivate(ctx, &task)
	}
	return nil
}
