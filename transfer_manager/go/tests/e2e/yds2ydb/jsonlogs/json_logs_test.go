package jsonlogs

import (
	"context"
	"os"
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
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/ydb"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/yds"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/runtime/local"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers/lbenv"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func TestPushClientLogs(t *testing.T) {
	lbEnv, stop := lbenv.NewLbEnv(t)
	//create logs pusher
	loggerPort := lbEnv.ProducerOptions().Port
	sourcePort := lbEnv.ConsumerOptions().Port

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "YDS source", Port: sourcePort},
			helpers.LabeledPort{Label: "Logger LB writer", Port: loggerPort},
		))
	}()
	defer stop()

	lgr, err := logger.NewLogbrokerLoggerFromConfig(&logger.LogbrokerConfig{
		Instance:    lbEnv.ProducerOptions().Endpoint,
		Port:        loggerPort,
		Topic:       lbEnv.DefaultTopic,
		SourceID:    "test",
		Credentials: lbEnv.ProducerOptions().Credentials,
	}, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	// create source
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

	src := &yds.YDSSource{
		Endpoint:    lbEnv.Endpoint,
		Port:        sourcePort,
		Database:    "",
		Stream:      lbEnv.DefaultTopic,
		Consumer:    lbEnv.DefaultConsumer,
		Credentials: lbEnv.Creds,

		SupportedCodecs: nil,
		AllowTTLRewind:  false,

		ParserConfig: parserConfigMap,
	}

	// create destination
	endpoint, ok := os.LookupEnv("YDB_ENDPOINT")
	if !ok {
		t.Fail()
	}
	targetPort, err := helpers.GetPortFromStr(endpoint)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "YDB target", Port: targetPort},
		))
	}()

	prefix, ok := os.LookupEnv("YDB_DATABASE")
	if !ok {
		t.Fail()
	}

	token, ok := os.LookupEnv("YDB_TOKEN")
	if !ok {
		token = "anyNotEmptyString"
	}

	dst := &ydb.YdbDestination{
		Token:                 server.SecretString(token),
		Database:              prefix,
		Path:                  "",
		Instance:              endpoint,
		ShardCount:            0,
		Rotation:              nil,
		AltNames:              nil,
		Cleanup:               "",
		IsTableColumnOriented: false,
		DefaultCompression:    "off",
	}
	// push logs
	go func() {
		for i := 0; i < 50; i++ {
			lgr.Infof("line:%v", i)
		}
	}()
	// create transfer
	helpers.InitSrcDst(helpers.TransferID, src, dst, abstract.TransferTypeIncrementOnly)
	transfer := helpers.MakeTransfer(helpers.TransferID, src, dst, abstract.TransferTypeIncrementOnly)

	w := local.NewLocalWorker(
		coordinator.NewFakeClient(),
		transfer,
		solomon.NewRegistry(solomon.NewRegistryOpts()),
		logger.LoggerWithLevel(zapcore.DebugLevel),
	)
	w.Start()
	// check results
	tableID := abstract.TableID{
		Namespace: "",
		Name:      strings.ReplaceAll(lbEnv.DefaultTopic, "/", "_"),
	}
	dstStorage, err := ydb.NewStorage(dst.ToStorageParams())
	require.NoError(t, err)
	err = helpers.WaitDestinationEqualRowsCount(tableID.Namespace, tableID.Name, dstStorage, time.Second*30, 50)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()
	schema, err := dstStorage.TableSchema(ctx, tableID)
	require.NoError(t, err)
	require.Len(t, schema.Columns(), 8)
}
