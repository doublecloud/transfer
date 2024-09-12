package main

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/clickhouse"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/clickhouse/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/yds"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/runtime/local"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers/lbenv"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestReplication(t *testing.T) {
	lbEnv, stop := lbenv.NewLbEnv(t)
	defer stop()
	lbSendingPort := lbEnv.ProducerOptions().Port
	lbReceivingPort := lbEnv.Port
	targetNativePort := helpers.GetIntFromEnv("DB0_RECIPE_CLICKHOUSE_NATIVE_PORT")

	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "LB logger", Port: lbReceivingPort},
		helpers.LabeledPort{Label: "CH target", Port: targetNativePort},
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

	dst := &model.ChDestination{
		ShardsList: []model.ClickHouseShard{
			{
				Name: "_",
				Hosts: []string{
					"localhost",
				},
			},
		},
		User:                "default",
		Password:            "",
		Database:            "raw",
		HTTPPort:            helpers.GetIntFromEnv("DB0_RECIPE_CLICKHOUSE_HTTP_PORT"),
		ProtocolUnspecified: true,
		NativePort:          targetNativePort,
	}
	dst.WithDefaults()

	// activate transfer
	helpers.InitSrcDst(helpers.TransferID, &src, dst, abstract.TransferTypeIncrementOnly)
	transfer := helpers.MakeTransfer(helpers.TransferID, &src, dst, abstract.TransferTypeIncrementOnly)

	st := time.Now()
	time.Sleep(time.Second)
	bullets := 100_000
	for i := 0; i < bullets; i++ {
		loggerLbWriter.Infof("some test info message: %d", i)
	}

	localWorker := local.NewLocalWorker(coordinator.NewFakeClient(), transfer, solomon.NewRegistry(solomon.NewRegistryOpts()), logger.LoggerWithLevel(zapcore.DebugLevel))
	localWorker.Start()
	defer localWorker.Stop()
	time.Sleep(5 * time.Second)
	db, err := clickhouse.MakeConnection(dst.ToStorageParams())
	require.NoError(t, err)
	require.NoError(t, helpers.WaitDestinationEqualRowsCount(
		"raw",
		strings.ReplaceAll(src.Stream, "-", "_"),
		helpers.GetSampleableStorageByModel(t, dst),
		2*time.Minute,
		uint64(bullets),
	))
	var data *string
	var topic, partition string
	var seqNo int64
	var wTime time.Time
	require.NoError(t,
		db.QueryRow(
			fmt.Sprintf(
				"select data, topic, partition, seq_no, write_time from `%v` order by seq_no desc limit 1",
				strings.ReplaceAll(src.Stream, "-", "_"),
			),
		).Scan(&data, &topic, &partition, &seqNo, &wTime))
	require.NotNil(t, data)
	require.Contains(t, *data, fmt.Sprintf("some test info message: %d", bullets-1))
	require.Equal(t, src.Stream, topic)
	require.NotEqual(t, "", partition)
	require.Equal(t, int64(bullets-1), seqNo)
	require.Less(t, wTime, time.Now())
	require.Greater(t, wTime, st)
}
