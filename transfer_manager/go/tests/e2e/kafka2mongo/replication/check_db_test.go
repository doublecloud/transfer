package main

import (
	"os"
	"testing"
	"time"

	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers"
	jsonparser "github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers/registry/json"
	kafkasink "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/kafka"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/logbroker"
	mongodataagent "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/mongo"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/stretchr/testify/require"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

var (
	source = kafkasink.KafkaSource{
		Connection: &kafkasink.KafkaConnectionOptions{
			TLS:     logbroker.DisabledTLS,
			Brokers: []string{os.Getenv("KAFKA_RECIPE_BROKER_LIST")},
		},
		Auth:             &kafkasink.KafkaAuth{Enabled: false},
		Topic:            "topic1",
		Transformer:      nil,
		BufferSize:       server.BytesSize(1024),
		SecurityGroupIDs: nil,
		ParserConfig:     nil,
		IsHomo:           false,
	}
	target = mongodataagent.MongoDestination{
		Hosts:    []string{"localhost"},
		Port:     helpers.GetIntFromEnv("MONGO_LOCAL_PORT"),
		Database: "db1",
		User:     os.Getenv("MONGO_LOCAL_USER"),
		Password: server.SecretString(os.Getenv("MONGO_LOCAL_PASSWORD")),
		Cleanup:  server.Drop,
	}
)

func TestReplication(t *testing.T) {
	// prepare source

	parserConfigStruct := &jsonparser.ParserConfigJSONCommon{
		Fields: []abstract.ColSchema{
			{ColumnName: "id", DataType: ytschema.TypeInt32.String(), PrimaryKey: true},
			{ColumnName: "level", DataType: ytschema.TypeString.String()},
			{ColumnName: "caller", DataType: ytschema.TypeString.String()},
			{ColumnName: "msg", DataType: ytschema.TypeString.String()},
		},
		AddRest:       false,
		AddDedupeKeys: true,
	}
	parserConfigMap, err := parsers.ParserConfigStructToMap(parserConfigStruct)
	require.NoError(t, err)

	source.ParserConfig = parserConfigMap

	// write to source topic

	k := []byte(`any_key`)
	v := []byte(`{"id": "1", "level": "my_level", "caller": "my_caller", "msg": "my_msg"}`)

	srcSink, err := kafkasink.NewReplicationSink(
		&kafkasink.KafkaDestination{
			Connection: source.Connection,
			Auth:       source.Auth,
			Topic:      source.Topic,
			FormatSettings: server.SerializationFormat{
				Name: server.SerializationFormatJSON,
				BatchingSettings: &server.Batching{
					Enabled:        false,
					Interval:       0,
					MaxChangeItems: 0,
					MaxMessageSize: 0,
				},
			},
			ParralelWriterCount: 10,
		},
		solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()}),
		logger.Log,
	)
	require.NoError(t, err)
	err = srcSink.Push([]abstract.ChangeItem{kafkasink.MakeKafkaRawMessage(source.Topic, time.Time{}, source.Topic, 0, 0, k, v)})
	require.NoError(t, err)

	// activate transfer

	transfer := helpers.MakeTransfer(helpers.TransferID, &source, &target, abstract.TransferTypeIncrementOnly)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	// check results

	require.NoError(t, helpers.WaitDestinationEqualRowsCount(
		target.Database,
		"topic1",
		helpers.GetSampleableStorageByModel(t, target),
		60*time.Second,
		1,
	))
}
