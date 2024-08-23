package main

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/yds"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/yt"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/runtime/local"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers/lbenv"
	yt_helpers "github.com/doublecloud/transfer/transfer_manager/go/tests/helpers/yt"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

type parsedYtRow struct {
	Level  string
	TS     string
	Caller string
	Msg    string
}

type RawYtRow struct {
	Data string `yson:"data"`
}

func TestReplication(t *testing.T) {
	lbEnv, stop := lbenv.NewLbEnv(t)
	defer stop()
	lbSendingPort := lbEnv.ProducerOptions().Port
	lbReceivingPort := lbEnv.Port

	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "LB logger", Port: lbReceivingPort},
		helpers.LabeledPort{Label: "YDS source", Port: lbReceivingPort},
	))

	require.NoError(t, os.Setenv("CONSOLE_LOG_LEVEL", "WARN")) // to reduce spam for stdout
	loggerLbWriter, err := logger.NewLogbrokerLoggerFromConfig(&logger.LogbrokerConfig{
		Instance:    lbEnv.ProducerOptions().Endpoint,
		Port:        lbSendingPort,
		Topic:       lbEnv.DefaultTopic,
		SourceID:    "test",
		Credentials: lbEnv.ProducerOptions().Credentials,
	}, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	src := yds.YDSSource{
		Endpoint:       lbEnv.Endpoint,
		Port:           lbReceivingPort,
		Database:       "",
		Stream:         lbEnv.DefaultTopic,
		Consumer:       lbEnv.DefaultConsumer,
		Credentials:    lbEnv.Creds,
		S3BackupBucket: "",
		BackupMode:     "",
		Transformer:    nil,
		ParserConfig:   nil,
	}

	ytPath := "//home/cdc/test/yds2yt_e2e"
	ytCluster := os.Getenv("YT_PROXY")
	dst := yt.NewYtDestinationV1(yt.YtDestination{
		Path:                     ytPath,
		Cluster:                  ytCluster,
		CellBundle:               "default",
		PrimaryMedium:            "default",
		UseStaticTableOnSnapshot: false, // TM-4444
		Cleanup:                  "Disabled",
	})
	dst.WithDefaults()

	transferType := abstract.TransferTypeIncrementOnly
	helpers.InitSrcDst(helpers.TransferID, &src, dst, transferType)
	transfer := helpers.MakeTransfer(helpers.TransferID, &src, dst, transferType)

	// send to logbroker
	bullets := 100_000
	msgTmpl := "some test info message"
	go func() {
		for i := 0; i < bullets; i++ {
			loggerLbWriter.Infof(msgTmpl+": %d", i)
		}
	}()

	localWorker := local.NewLocalWorker(
		coordinator.NewFakeClient(),
		transfer,
		solomon.NewRegistry(solomon.NewRegistryOpts()),
		logger.LoggerWithLevel(zapcore.DebugLevel),
	)
	localWorker.Start()
	defer localWorker.Stop()

	// wait to complete and check results
	require.NoError(t, helpers.WaitDestinationEqualRowsCount(
		"",
		src.Stream,
		helpers.GetSampleableStorageByModel(t, dst),
		time.Minute,
		uint64(bullets),
	))

	rowsWritten := yt_helpers.YtReadAllRowsFromAllTables[RawYtRow](t, ytCluster, ytPath, bullets)
	require.NotEmpty(t, rowsWritten)
	for _, unparsedRow := range rowsWritten {
		var row parsedYtRow
		err := json.Unmarshal([]byte(unparsedRow.Data), &row)
		require.NoError(t, err)

		parts := strings.Split(row.Msg, ": ")
		require.Equal(t, 2, len(parts))
		require.Equal(t, parts[0], msgTmpl)
		msgNo, err := strconv.ParseInt(parts[1], 10, 0)
		require.NoError(t, err)
		require.True(t, msgNo >= 0)
		require.True(t, int(msgNo) < bullets)

		require.Equal(t, "INFO", row.Level)
		require.NotEmpty(t, row.TS)
		require.NotEmpty(t, row.Caller)
	}
}
