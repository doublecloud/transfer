package tasks

import (
	"context"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/metrics"
	"github.com/doublecloud/transfer/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/providers"
)

func Deactivate(ctx context.Context, cp coordinator.Coordinator, transfer server.Transfer, task server.TransferOperation, registry metrics.Registry) error {
	deactivator, ok := providers.Source[providers.Deactivator](logger.Log, registry, cp, &transfer)
	if ok {
		return deactivator.Deactivate(ctx, &task)
	}
	return nil
}
