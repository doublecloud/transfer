package jsonlogs

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers"
	jsonparser "github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers/registry/json"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/yds"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/yt"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/runtime/local"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers/lbenv"
	yt_helpers "github.com/doublecloud/transfer/transfer_manager/go/tests/helpers/yt"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

type RawYtRow struct {
	Level  string `yson:"level"`
	TS     string `yson:"ts"`
	Caller string `yson:"caller"`
	Msg    string `yson:"msg"`
}

type msg struct {
	ID    string
	Bytes int
}

func TestPushClientLogs(t *testing.T) {
	lbEnv, stop := lbenv.NewLbEnv(t)

	loggerPort := lbEnv.ProducerOptions().Port
	sourcePort := lbEnv.Port

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "LB logger", Port: loggerPort},
			helpers.LabeledPort{Label: "YDS source", Port: sourcePort},
		))
	}()
	defer stop()

	require.NoError(t, os.Setenv("CONSOLE_LOG_LEVEL", "WARN")) // to reduce spam for stdout
	loggerLbWriter, err := logger.NewLogbrokerLoggerFromConfig(&logger.LogbrokerConfig{
		Instance:    lbEnv.ProducerOptions().Endpoint,
		Port:        loggerPort,
		Topic:       lbEnv.DefaultTopic,
		SourceID:    "test",
		Credentials: lbEnv.ProducerOptions().Credentials,
	}, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	parserConfigStruct := &jsonparser.ParserConfigJSONCommon{
		Fields: []abstract.ColSchema{
			{ColumnName: "ts", DataType: ytschema.TypeString.String(), PrimaryKey: true},
			{ColumnName: "level", DataType: ytschema.TypeString.String()},
			{ColumnName: "caller", DataType: ytschema.TypeString.String()},
			{ColumnName: "msg", DataType: ytschema.TypeString.String()},
		},
		AddRest:       false,
		AddDedupeKeys: true,
	}
	parserConfigMap, err := parsers.ParserConfigStructToMap(parserConfigStruct)
	require.NoError(t, err)

	src := yds.YDSSource{
		Endpoint:       lbEnv.Endpoint,
		Port:           sourcePort,
		Database:       "",
		Stream:         lbEnv.DefaultTopic,
		Consumer:       lbEnv.DefaultConsumer,
		Credentials:    lbEnv.Creds,
		S3BackupBucket: "",
		BackupMode:     "",
		Transformer:    nil,
		ParserConfig:   parserConfigMap,
	}

	ytPath := "//home/cdc/test/ydsjson2yt_e2e"
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
	bullets := 50
	go func() {
		for i := 0; i < bullets; i++ {
			loggerLbWriter.Infof(fmt.Sprintf(`{"ID": "--%d--", "Bytes": %d}`, i, i))
		}
	}()

	w := local.NewLocalWorker(
		coordinator.NewFakeClient(),
		transfer,
		solomon.NewRegistry(solomon.NewRegistryOpts()),
		logger.LoggerWithLevel(zapcore.DebugLevel),
	)
	w.Start()

	// wait to complete and check results
	require.NoError(t, helpers.WaitDestinationEqualRowsCount(
		"",
		lbEnv.DefaultTopic,
		helpers.GetSampleableStorageByModel(t, dst),
		time.Minute,
		uint64(bullets),
	))

	rowsWritten := yt_helpers.YtReadAllRowsFromAllTables[RawYtRow](t, ytCluster, ytPath, bullets)
	require.NotEmpty(t, rowsWritten)
	for _, row := range rowsWritten {
		var curMsg msg
		err := json.Unmarshal([]byte(row.Msg), &curMsg)
		require.NoError(t, err)

		require.True(t, curMsg.Bytes >= 0)
		require.True(t, int(curMsg.Bytes) < bullets)
		require.Equal(t, fmt.Sprintf("--%d--", curMsg.Bytes), curMsg.ID)

		require.Equal(t, "INFO", row.Level)
		require.NotEmpty(t, row.TS)
		require.NotEmpty(t, row.Caller)
		require.NotEmpty(t, row.Msg)
	}
}
