package main

import (
	"testing"
	"time"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/pkg/abstract"
	server "github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/parsers"
	jsonparser "github.com/doublecloud/transfer/pkg/parsers/registry/json"
	chrecipe "github.com/doublecloud/transfer/pkg/providers/clickhouse/recipe"
	kafkasink "github.com/doublecloud/transfer/pkg/providers/kafka"
	"github.com/doublecloud/transfer/tests/canon/reference"
	"github.com/doublecloud/transfer/tests/helpers"
	"github.com/stretchr/testify/require"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

var (
	kafkaTopic = "topic1"
	source     = *kafkasink.MustSourceRecipe()

	chDatabase     = "public"
	target         = *chrecipe.MustTarget(chrecipe.WithInitDir("dump/ch"), chrecipe.WithDatabase(chDatabase))
	targetAsSource = *chrecipe.MustSource(chrecipe.WithInitDir("dump/ch"), chrecipe.WithDatabase(chDatabase))

	timestampToUse = time.Date(2024, 03, 19, 0, 0, 0, 0, time.Local)
)

func includeAllTables(table abstract.TableID, schema abstract.TableColumns) bool {
	return true
}

func fixTimestampMiddleware(t *testing.T, items []abstract.ChangeItem) abstract.TransformerResult {
	for _, item := range items {
		for i, name := range item.ColumnNames {
			if name == "_timestamp" {
				// Fix timestamp to support canonization
				item.ColumnValues[i] = timestampToUse
				break
			}
		}
	}

	return abstract.TransformerResult{
		Transformed: items,
	}
}

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
	source.Topic = kafkaTopic

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
	// add transformation in order to control Kafka timestamp
	err = transfer.AddExtraTransformer(helpers.NewSimpleTransformer(t, fixTimestampMiddleware, includeAllTables))
	require.NoError(t, err)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	// check results

	require.NoError(t, helpers.WaitDestinationEqualRowsCount(
		target.Database,
		kafkaTopic,
		helpers.GetSampleableStorageByModel(t, target),
		60*time.Second,
		1,
	))
	reference.Dump(t, &targetAsSource)
}
