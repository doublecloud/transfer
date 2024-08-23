package mirror

import (
	"fmt"
	"strings"
	"testing"

	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers"
	blankparser "github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers/registry/blank"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/logbroker"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers/lbenv"
	"github.com/stretchr/testify/require"
)

func TestPushClientLogs(t *testing.T) {
	lbEnv, stop := lbenv.NewLbEnv(t)
	defer stop()
	lbSendingPort := lbEnv.ProducerOptions().Port
	lbReceivingPort := lbEnv.Port

	//------------------------------------------------------------------------------------
	// prepare producer

	loggerLbWriter, err := logger.NewLogbrokerLoggerFromConfig(&logger.LogbrokerConfig{
		Instance:    lbEnv.ProducerOptions().Endpoint,
		Port:        lbSendingPort,
		Topic:       "src/topic",
		SourceID:    "test",
		Credentials: lbEnv.ProducerOptions().Credentials,
	}, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	// prepare src

	parserConfigMap, err := parsers.ParserConfigStructToMap(&blankparser.ParserConfigBlankLb{})
	require.NoError(t, err)

	src := logbroker.LfSource{
		Instance:     logbroker.LogbrokerInstance(lbEnv.Endpoint),
		Topics:       []string{"src/topic"},
		Credentials:  lbEnv.ConsumerOptions().Credentials,
		Consumer:     lbEnv.DefaultConsumer,
		Port:         lbReceivingPort,
		ParserConfig: parserConfigMap,
	}

	// prepare dst

	dst := logbroker.LbDestination{
		Instance:    lbEnv.Endpoint,
		Topic:       "dst/topic",
		Credentials: lbEnv.ConsumerOptions().Credentials,
		Port:        lbReceivingPort,
		FormatSettings: server.SerializationFormat{
			Name: server.SerializationFormatAuto,
		},
		TLS: logbroker.DisabledTLS,
	}

	helpers.InitSrcDst(helpers.TransferID, &src, &dst, abstract.TransferTypeIncrementOnly)
	transfer := helpers.MakeTransfer(helpers.TransferID, &src, &dst, abstract.TransferTypeIncrementOnly)
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	// send message to logbroker

	loggerLbWriter.Info("my_lovely_message")

	// read

	dataCmp := func(in string, index int) bool {
		fmt.Printf("dataCmp: received string:%s\n", in)
		return lbenv.EqualLogLineContent(t, `{"level":"INFO","msg":"my_lovely_message"}`, in)
	}

	extraCmp := func(in string, index int) bool {
		fmt.Printf("extraCmp received string:%s\n", in)
		return strings.Contains(in, `"ident":"src"`) && strings.Contains(in, `"logtype":"topic"`)
	}

	ipCmp := func(in string, index int) bool {
		fmt.Printf("ipCmp received string:%s\n", in)
		return len(in) > 0
	}

	batchTopicCmp := func(in string, index int) bool {
		fmt.Printf("batchTopicCmp received string:%s\n", in)
		return in == "rt3.dc1--dst--topic"
	}

	lbenv.CheckResult(t, lbEnv.Env, dst.Database, dst.Topic, 1, dataCmp, extraCmp, lbenv.ComparatorStub, ipCmp, batchTopicCmp)
}
