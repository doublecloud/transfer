package postgres

import (
	"testing"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/internal/metrics"
	"github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/middlewares"
	"github.com/stretchr/testify/require"
)

func TestSnapshotTurnOffPerTransactionPush(t *testing.T) {
	dst := &PgDestination{
		Hosts: []string{
			"localhost:0",
		},
		PerTransactionPush: true,
	}

	transfer := new(model.Transfer)
	transfer.Dst = dst

	provider := New(logger.Log, metrics.NewRegistry(), nil, transfer).(*Provider)

	_, _ = provider.SnapshotSink(middlewares.Config{})
	require.False(t, transfer.Dst.(*PgDestination).PerTransactionPush)
}
