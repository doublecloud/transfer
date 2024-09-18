package downtime

import (
	"testing"
	"time"

	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/pkg/providers/clickhouse"
	"github.com/doublecloud/transfer/pkg/providers/clickhouse/model"
	"github.com/doublecloud/transfer/pkg/stats"
	"github.com/doublecloud/transfer/tests/helpers"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"go.ytsaurus.tech/library/go/core/log/zap"
)

type TestParams struct {
	Host  string
	Alive bool
}

func test(t *testing.T, params TestParams) {
	var config model.ChDestination
	config.User = "default"
	config.Password = ""
	config.Database = "downtime_test"
	config.HTTPPort = helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT")
	config.NativePort = helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT")
	config.WithDefaults()
	onPingCalled := false

	storageStats := stats.NewChStats(solomon.NewRegistry(nil))

	sinkServer, err := clickhouse.NewSinkServerImpl(
		config.ToReplicationFromPGSinkParams().MakeChildServerParams(params.Host),
		&zap.Logger{L: zaptest.NewLogger(t)},
		storageStats,
		nil,
	)
	require.NoError(t, err)

	sinkServerEvents := &clickhouse.SinkServerCallbacks{}
	sinkServerEvents.OnPing = func(sinkServer *clickhouse.SinkServer) {
		require.Equal(t, params.Alive, sinkServer.Alive())
		onPingCalled = true
	}
	sinkServer.TestSetCallbackOnPing(sinkServerEvents)
	sinkServer.RunGoroutines()

	if sinkServer.Alive() {
		require.Equal(t, params.Alive, sinkServer.Alive())
	}

	time.Sleep(1 * time.Second)

	require.True(t, onPingCalled)
}

func TestAliveNoZK(t *testing.T) {
	test(t, TestParams{Host: "localhost", Alive: true})
}

func TestDowntime(t *testing.T) {
	test(t, TestParams{Host: "fake", Alive: false})
}
