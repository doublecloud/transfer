package jsonlogs

import (
	"strings"
	"testing"
	"time"

	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers"
	jsonparser "github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers/registry/json"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/clickhouse/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/logbroker"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/runtime/local"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers/lbenv"
	"github.com/stretchr/testify/require"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func TestPushClientLogs(t *testing.T) {
	lbEnv, stop := lbenv.NewLbEnv(t)

	sourcePort := lbEnv.ConsumerOptions().Port
	loggerPort := lbEnv.ProducerOptions().Port
	targetNativePort := helpers.GetIntFromEnv("DB0_RECIPE_CLICKHOUSE_NATIVE_PORT")

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "LF source", Port: sourcePort},
			helpers.LabeledPort{Label: "Logger LB writer", Port: loggerPort},
			helpers.LabeledPort{Label: "CH target", Port: targetNativePort},
		))
	}()

	defer stop()
	loggerLbWriter, err := logger.NewLogbrokerLoggerFromConfig(&logger.LogbrokerConfig{
		Instance:    lbEnv.ProducerOptions().Endpoint,
		Port:        loggerPort,
		Topic:       lbEnv.DefaultTopic,
		SourceID:    "test",
		Credentials: lbEnv.ProducerOptions().Credentials,
	}, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	parserConfigStruct := &jsonparser.ParserConfigJSONLb{
		Fields: []abstract.ColSchema{
			{ColumnName: "msg", DataType: ytschema.TypeString.String()},
		},
		AddRest: false,
	}
	parserConfigMap, err := parsers.ParserConfigStructToMap(parserConfigStruct)
	require.NoError(t, err)

	src := &logbroker.LfSource{
		Instance:     logbroker.LogbrokerInstance(lbEnv.Endpoint),
		Topics:       []string{lbEnv.DefaultTopic},
		Credentials:  lbEnv.ConsumerOptions().Credentials,
		Consumer:     lbEnv.DefaultConsumer,
		Port:         sourcePort,
		ParserConfig: parserConfigMap,
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
		Database:            "mtmobproxy",
		HTTPPort:            helpers.GetIntFromEnv("DB0_RECIPE_CLICKHOUSE_HTTP_PORT"),
		NativePort:          targetNativePort,
		ProtocolUnspecified: true,
	}
	dst.WithDefaults()
	transfer := &server.Transfer{
		ID:  "e2e_test",
		Src: src,
		Dst: dst,
	}
	// SEND TO LOGBROKER
	go func() {
		for i := 0; i < 50; i++ {
			loggerLbWriter.Infof("line:%v", i)
		}
	}()
	w := local.NewLocalWorker(coordinator.NewFakeClient(), transfer, solomon.NewRegistry(solomon.NewRegistryOpts()), logger.Log)
	w.Start()
	require.NoError(t, helpers.WaitDestinationEqualRowsCount(dst.Database, strings.ReplaceAll(lbEnv.DefaultTopic, "-", "_"), helpers.GetSampleableStorageByModel(t, dst), 60*time.Second, 50))
}
