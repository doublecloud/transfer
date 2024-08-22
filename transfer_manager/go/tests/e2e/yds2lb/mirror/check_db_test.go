package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/logbroker"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/yds"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/runtime/local"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers/lbenv"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

//---------------------------------------------------------------------------------------------------------------------

func TestReplication(t *testing.T) {
	lbEnv, stop := lbenv.NewLbEnv(t)
	defer stop()
	lbSendingPort := lbEnv.ProducerOptions().Port
	lbReceivingPort := lbEnv.Port

	// prepare producer

	loggerLbWriter, err := logger.NewLogbrokerLoggerFromConfig(&logger.LogbrokerConfig{
		Instance:    lbEnv.ProducerOptions().Endpoint,
		Port:        lbSendingPort,
		Topic:       lbEnv.DefaultTopic,
		SourceID:    "test",
		Credentials: lbEnv.ProducerOptions().Credentials,
	}, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	// prepare src

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

	// activate transfer

	helpers.InitSrcDst(helpers.TransferID, &src, &dst, abstract.TransferTypeIncrementOnly)
	transfer := helpers.MakeTransfer(helpers.TransferID, &src, &dst, abstract.TransferTypeIncrementOnly)

	localWorker := local.NewLocalWorker(coordinator.NewFakeClient(), transfer, solomon.NewRegistry(solomon.NewRegistryOpts()), logger.LoggerWithLevel(zapcore.DebugLevel))
	localWorker.Start()
	defer localWorker.Stop()

	// send message to logbroker

	loggerLbWriter.Info("my_lovely_message")

	// read

	dataCmp := func(in string, index int) bool {
		fmt.Printf("received string:%s\n", in)
		return lbenv.EqualLogLineContent(t, `{"level":"INFO","msg":"my_lovely_message"}`, in)
	}

	lbenv.CheckResult(t, lbEnv.Env, dst.Database, dst.Topic, 1, dataCmp, lbenv.ComparatorStub, lbenv.ComparatorStub, lbenv.ComparatorStub, lbenv.ComparatorStub)
}
