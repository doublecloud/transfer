package main

import (
	"os"
	"testing"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/pkg/abstract"
	server "github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/debezium"
	debeziumparameters "github.com/doublecloud/transfer/pkg/debezium/parameters"
	"github.com/doublecloud/transfer/pkg/providers/ydb"
	"github.com/doublecloud/transfer/tests/helpers"
	"github.com/doublecloud/transfer/tests/helpers/serde"
	"github.com/stretchr/testify/require"
)

var path = "dectest/timmyb32r-test"
var pathOut = "dectest/timmyb32r-test-out"
var sourceChangeItem abstract.ChangeItem

func TestSnapshotAndSerDeViaDebeziumEmbedded(t *testing.T) {
	src := &ydb.YdbSource{
		Token:              server.SecretString(os.Getenv("YDB_TOKEN")),
		Database:           helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             nil,
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           false,
		ServiceAccountID:   "",
	}

	t.Run("init source database", func(t *testing.T) {
		Target := &ydb.YdbDestination{
			Database: src.Database,
			Token:    src.Token,
			Instance: src.Instance,
		}
		Target.WithDefaults()
		sinker, err := ydb.NewSinker(logger.Log, Target, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)

		currChangeItem := helpers.YDBInitChangeItem(path)
		for i := 1; i < len(currChangeItem.ColumnValues); i++ {
			currChangeItem.ColumnValues[i] = nil
		}
		require.NoError(t, sinker.Push([]abstract.ChangeItem{*currChangeItem}))
	})

	dst := &ydb.YdbDestination{
		Token:    server.SecretString(os.Getenv("YDB_TOKEN")),
		Database: helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance: helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
	}
	dst.WithDefaults()
	transfer := helpers.MakeTransfer("fake", src, dst, abstract.TransferTypeSnapshotOnly)

	emitter, err := debezium.NewMessagesEmitter(map[string]string{
		debeziumparameters.DatabaseDBName:   "public",
		debeziumparameters.TopicPrefix:      "my_topic",
		debeziumparameters.AddOriginalTypes: "true",
	}, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)
	receiver := debezium.NewReceiver(nil, nil)
	debeziumSerDeTransformer := helpers.NewSimpleTransformer(t, serde.MakeYdb2YdbDebeziumSerDeUdf(pathOut, &sourceChangeItem, emitter, receiver), serde.AnyTablesUdf)
	helpers.AddTransformer(t, transfer, debeziumSerDeTransformer)

	t.Run("activate", func(t *testing.T) {
		helpers.Activate(t, transfer)
	})

	//-----------------------------------------------------------------------------------------------------------------
	// check

	sinkMock := &helpers.MockSink{}
	targetMock := server.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinkMock },
		Cleanup:       server.DisabledCleanup,
	}
	transferMock := helpers.MakeTransfer("fake", src, &targetMock, abstract.TransferTypeSnapshotOnly)
	var extractedChangeItem abstract.ChangeItem
	t.Run("extract change_item from dst", func(t *testing.T) {
		sinkMock.PushCallback = func(input []abstract.ChangeItem) {
			for _, currItem := range input {
				if currItem.Table == pathOut && currItem.Kind == abstract.InsertKind {
					extractedChangeItem = currItem
				}
			}
		}
		helpers.Activate(t, transferMock)
	})
	sourceKeys := sourceChangeItem.KeysAsMap()
	for i := 0; i < len(sourceChangeItem.ColumnValues); i++ {
		if _, ok := sourceKeys[sourceChangeItem.ColumnNames[i]]; ok {
			continue
		}
		require.Nil(t, sourceChangeItem.ColumnValues[i])
	}
	sourceChangeItem.CommitTime = 0
	sourceChangeItem.Table = "!"
	sourceChangeItem.PartID = ""
	sourceChangeItemStr := sourceChangeItem.ToJSONString()
	logger.Log.Infof("sourceChangeItemStr:%s\n", sourceChangeItemStr)

	extractedKeys := extractedChangeItem.KeysAsMap()
	for i := 0; i < len(extractedChangeItem.ColumnValues); i++ {
		if _, ok := extractedKeys[extractedChangeItem.ColumnNames[i]]; ok {
			continue
		}
		require.Nil(t, extractedChangeItem.ColumnValues[i])
	}

	extractedChangeItem.CommitTime = 0
	extractedChangeItem.Table = "!"
	extractedChangeItem.PartID = ""
	extractedChangeItemStr := extractedChangeItem.ToJSONString()
	logger.Log.Infof("extractedChangeItemStr:%s\n", extractedChangeItemStr)
	require.Equal(t, sourceChangeItemStr, extractedChangeItemStr)
}
