package mirror

import (
	"testing"

	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers/registry/json"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/logbroker"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/transformer/registry/filter"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers/lbenv"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/library/go/core/log"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func TestPushClientLogs(t *testing.T) {
	lbEnv, stop := lbenv.NewLbEnv(t)
	defer stop()
	lbSendingPort := lbEnv.ProducerOptions().Port
	lbReceivingPort := lbEnv.Port

	loggerLbWriter, err := logger.NewLogbrokerLoggerFromConfig(&logger.LogbrokerConfig{
		Instance:    lbEnv.ProducerOptions().Endpoint,
		Port:        lbSendingPort,
		Topic:       "src/topic",
		SourceID:    "test",
		Credentials: lbEnv.ProducerOptions().Credentials,
	}, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	parserConfigMap, err := parsers.ParserConfigStructToMap(&json.ParserConfigJSONLb{
		Fields: []abstract.ColSchema{
			{ColumnName: "level", DataType: ytschema.TypeString.String(), PrimaryKey: true}, // PrimaryKey:true - is a workaround to 'CustomFilterColumnsTransformer' work
			{ColumnName: "caller", DataType: ytschema.TypeString.String()},
			{ColumnName: "ts", DataType: ytschema.TypeTimestamp.String()},
			{ColumnName: "array", DataType: ytschema.TypeAny.String()},
		},
		SkipSystemKeys: true,
		AddRest:        true,
	})
	require.NoError(t, err)

	src := logbroker.LfSource{
		Instance:     logbroker.LogbrokerInstance(lbEnv.Endpoint),
		Topics:       []string{"src/topic"},
		Credentials:  lbEnv.ConsumerOptions().Credentials,
		Consumer:     lbEnv.DefaultConsumer,
		Port:         lbReceivingPort,
		ParserConfig: parserConfigMap,
	}

	dst := logbroker.LbDestination{
		Instance:    lbEnv.Endpoint,
		Topic:       "dst/topic",
		Credentials: lbEnv.ConsumerOptions().Credentials,
		Port:        lbReceivingPort,
		FormatSettings: server.SerializationFormat{
			Name: server.SerializationFormatJSON,
		},
		TLS: logbroker.DisabledTLS,
	}

	helpers.InitSrcDst(helpers.TransferID, &src, &dst, abstract.TransferTypeIncrementOnly)
	transfer := helpers.MakeTransfer(helpers.TransferID, &src, &dst, abstract.TransferTypeIncrementOnly)
	tableF, err := filter.NewFilter(nil, nil)
	require.NoError(t, err)
	columnF, err := filter.NewFilter([]string{}, []string{"_timestamp", "_partition", "_offset", "_idx"})
	require.NoError(t, err)
	require.NoError(t, transfer.AddExtraTransformer(filter.NewCustomFilterColumnsTransformer(
		tableF,
		columnF,
		logger.Log,
	)))
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	loggerLbWriter.Info("My_message", log.Array("array", []string{}))

	dataCmp := func(in string, index int) bool {
		logger.Log.Infof("received string: %s\n", in)
		return lbenv.EqualLogLineContent(t, `{"_rest":{"msg":"My_message"},"array":[],"level":"INFO"}`, in)
	}

	lbenv.CheckResult(t, lbEnv.Env, dst.Database, dst.Topic, 1, dataCmp, lbenv.ComparatorStub, lbenv.ComparatorStub, lbenv.ComparatorStub, lbenv.ComparatorStub)
}
