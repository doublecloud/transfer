package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers"
	blankparser "github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers/registry/blank"
	kafka2 "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/kafka"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/logbroker"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/runtime/local"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers/lbenv"
	"github.com/golang/mock/gomock"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

//---------------------------------------------------------------------------------------------------------------------

var index = 0
var tt *testing.T

func callbackFunc(_ interface{}, msgs ...interface{}) error {
	for _, el := range msgs {
		switch v := el.(type) {
		case kafka.Message:
			var j map[string]string
			err := json.Unmarshal(v.Value, &j)
			require.NoError(tt, err)
			delete(j, "ts")
			delete(j, "caller")
			jj, err := json.Marshal(j)
			require.NoError(tt, err)

			require.Equal(tt, []byte(nil), v.Key)
			require.Equal(tt, `{"level":"INFO","msg":"blablabla"}`, string(jj))
		default:
			require.FailNow(tt, "")
		}
	}

	index++
	return nil
}

//---------------------------------------------------------------------------------------------------------------------

func TestReplication(t *testing.T) {
	lbEnv, stop := lbenv.NewLbEnv(t)
	defer stop()
	lbSendingPort := lbEnv.ProducerOptions().Port
	lbReceivingPort := lbEnv.Port

	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "PG source", Port: lbSendingPort},
	))

	//------------------------------------------------------------------------------

	tt = t

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

	parserConfigMap, err := parsers.ParserConfigStructToMap(&blankparser.ParserConfigBlankLb{})
	require.NoError(t, err)

	src := logbroker.LfSource{
		Instance:     logbroker.LogbrokerInstance(lbEnv.Endpoint),
		Topics:       []string{lbEnv.DefaultTopic},
		Credentials:  lbEnv.ConsumerOptions().Credentials,
		Consumer:     lbEnv.DefaultConsumer,
		Port:         lbReceivingPort,
		ParserConfig: parserConfigMap,
	}

	// prepare dst

	dst := kafka2.KafkaDestination{
		Connection: &kafka2.KafkaConnectionOptions{
			TLS:     logbroker.DefaultTLS,
			Brokers: []string{"my_broker_0"},
		},
		Auth: &kafka2.KafkaAuth{
			Enabled:   true,
			Mechanism: "SHA-512",
			User:      "user1",
			Password:  "qwert12345",
		},
		Topic: "foo_bar",
		FormatSettings: server.SerializationFormat{
			Name: server.SerializationFormatLbMirror,
		},
		ParralelWriterCount: 10,
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	producer := kafka2.NewMockwriter(ctrl)
	producer.EXPECT().WriteMessages(gomock.Any(), gomock.Any()).AnyTimes().Do(callbackFunc)
	producer.EXPECT().Close().AnyTimes()

	kafkaMockClient := kafka2.NewMockclient(ctrl)
	kafkaMockClient.EXPECT().BuildWriter([]string{"my_broker_0"}, gomock.Any(), gomock.Any(), gomock.Any()).Return(producer)
	kafkaMockClient.EXPECT().CreateTopicIfNotExist([]string{"my_broker_0"}, "foo_bar", gomock.Any(), gomock.Any(), gomock.Any())

	sink, err := kafka2.NewSinkImpl(
		&dst,
		solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()}),
		logger.Log,
		kafkaMockClient,
		false,
	)
	require.NoError(t, err)

	target := server.MockDestination{SinkerFactory: func() abstract.Sinker { return sink }}

	// activate transfer

	helpers.InitSrcDst(helpers.TransferID, &src, &dst, abstract.TransferTypeIncrementOnly)
	transfer := helpers.MakeTransfer(helpers.TransferID, &src, &target, abstract.TransferTypeIncrementOnly)

	localWorker := local.NewLocalWorker(coordinator.NewFakeClient(), transfer, solomon.NewRegistry(solomon.NewRegistryOpts()), logger.Log)
	localWorker.Start()
	defer localWorker.Stop()

	//-----------------------------------------------------------------------------------------------------------------
	// send to logbroker

	loggerLbWriter.Infof("blablabla")

	for {
		if index == 1 {
			break
		}
		time.Sleep(time.Second)
	}
}
