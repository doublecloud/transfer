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
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/logbroker"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/ydb"
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
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "LF source", Port: sourcePort},
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
		Database:           prefix,
		Token:              server.SecretString(token),
		Instance:           endpoint,
		DefaultCompression: "off",
	}
	dst.WithDefaults()
	transfer := &server.Transfer{
		ID:  "e2e_test",
		Src: src,
		Dst: dst,
	}
	go func() {
		for i := 0; i < 50; i++ {
			lgr.Infof("line:%v", i)
		}
	}()
	w := local.NewLocalWorker(coordinator.NewFakeClient(), transfer, solomon.NewRegistry(solomon.NewRegistryOpts()), logger.Log)
	w.Start()
	time.Sleep(time.Second * 40)
	dstStorage, err := ydb.NewStorage(dst.ToStorageParams())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pushed := false
	require.NoError(t, dstStorage.LoadTable(ctx, abstract.TableDescription{
		Name:   strings.ReplaceAll(lbEnv.DefaultTopic, "/", "_"),
		Schema: "",
		Filter: "",
		EtaRow: 0,
		Offset: 0,
	}, func(input []abstract.ChangeItem) error {
		if len(input) == 1 && input[0].Kind != abstract.InsertKind {
			return nil
		}
		pushed = true

		require.Len(t, input, 50)
		return nil
	}))
	require.True(t, pushed)
}
