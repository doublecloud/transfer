package main

import (
	"os"
	"testing"
	"time"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/debezium"
	debeziumparameters "github.com/doublecloud/transfer/pkg/debezium/parameters"
	"github.com/doublecloud/transfer/pkg/providers/ydb"
	"github.com/doublecloud/transfer/tests/helpers"
	"github.com/doublecloud/transfer/tests/helpers/serde"
	simple_transformer "github.com/doublecloud/transfer/tests/helpers/transformer"
	"github.com/stretchr/testify/require"
)

var path = "dectest/test-src"
var pathOut = "dectest/test-dst"

func TestSnapshotAndReplicationSerDeViaDebeziumEmbeddedNulls(t *testing.T) {
	src := &ydb.YdbSource{
		Token:              model.SecretString(os.Getenv("YDB_TOKEN")),
		Database:           helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             []string{path},
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           false,
		UseFullPaths:       true,
		ServiceAccountID:   "",
		ChangeFeedMode:     ydb.ChangeFeedModeNewImage,
	}

	Target := &ydb.YdbDestination{
		Database: src.Database,
		Token:    src.Token,
		Instance: src.Instance,
	}
	Target.WithDefaults()
	sinker, err := ydb.NewSinker(logger.Log, Target, solomon.NewRegistry(solomon.NewRegistryOpts()))

	require.NoError(t, err)

	currChangeItem := helpers.YDBStmtInsertNulls(t, path, 1)
	require.NoError(t, sinker.Push([]abstract.ChangeItem{*currChangeItem}))

	dst := &ydb.YdbDestination{
		Token:    model.SecretString(os.Getenv("YDB_TOKEN")),
		Database: helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance: helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
	}
	helpers.InitSrcDst("fake", src, dst, abstract.TransferTypeSnapshotAndIncrement)
	transfer := helpers.MakeTransfer("fake", src, dst, abstract.TransferTypeSnapshotAndIncrement)

	emitter, err := debezium.NewMessagesEmitter(map[string]string{
		debeziumparameters.DatabaseDBName:   "public",
		debeziumparameters.TopicPrefix:      "my_topic",
		debeziumparameters.AddOriginalTypes: "true",
	}, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)

	receiver := debezium.NewReceiver(nil, nil)
	debeziumSerDeTransformer := simple_transformer.NewSimpleTransformer(t, serde.MakeYdb2YdbDebeziumSerDeUdf(pathOut, nil, emitter, receiver), serde.AnyTablesUdf)
	require.NoError(t, transfer.AddExtraTransformer(debeziumSerDeTransformer))

	worker := helpers.Activate(t, transfer)

	//-----------------------------------------------------------------------------------------------------------------
	require.NoError(t, sinker.Push([]abstract.ChangeItem{
		*helpers.YDBStmtInsertNulls(t, path, 2),
		*helpers.YDBStmtInsertNulls(t, path, 3),
	}))
	require.NoError(t, helpers.WaitEqualRowsCountDifferentTables(t, "", path, "", pathOut, helpers.GetSampleableStorageByModel(t, src), helpers.GetSampleableStorageByModel(t, dst), 60*time.Second))
	worker.Close(t)

	helpers.YDBTwoTablesEqual(t,
		os.Getenv("YDB_TOKEN"),
		helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
		path, pathOut)

	dump := helpers.YDBPullDataFromTable(t,
		os.Getenv("YDB_TOKEN"),
		helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
		pathOut)
	for _, changeItem := range dump {
		keys := changeItem.KeysAsMap()
		for i := 0; i < len(changeItem.ColumnValues); i++ {
			if _, ok := keys[changeItem.ColumnNames[i]]; ok {
				continue
			}
			require.Nil(t, changeItem.ColumnValues[i])
		}
	}
}
