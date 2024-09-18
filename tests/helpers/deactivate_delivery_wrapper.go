package helpers

import (
	"context"
	"testing"

	server "github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/worker/tasks"
	"github.com/stretchr/testify/require"
)

func Deactivate(t *testing.T, transfer *server.Transfer, worker *Worker, onErrorCallback ...func(err error)) error {
	if len(onErrorCallback) == 0 {
		// append default callback checker: no error!
		onErrorCallback = append(onErrorCallback, func(err error) {
			require.NoError(t, err)
		})
	}
	return DeactivateErr(transfer, worker, onErrorCallback...)
}

func DeactivateErr(transfer *server.Transfer, worker *Worker, onErrorCallback ...func(err error)) error {
	return tasks.Deactivate(context.Background(), worker.cp, *transfer, server.TransferOperation{}, EmptyRegistry())
}
