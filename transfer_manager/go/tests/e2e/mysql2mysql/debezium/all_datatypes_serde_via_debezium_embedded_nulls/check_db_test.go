package main

import (
	"os"
	"testing"
	"time"

	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/debezium"
	debeziumparameters "github.com/doublecloud/transfer/transfer_manager/go/pkg/debezium/parameters"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/mysql"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers/serde"
	"github.com/stretchr/testify/require"
)

var (
	Source = *helpers.RecipeMysqlSource()
	Target = *helpers.RecipeMysqlTarget()
)

func init() {
	_ = os.Setenv("YC", "1")                                                                            // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestSnapshotAndIncrement(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mysql source", Port: Source.Port},
			helpers.LabeledPort{Label: "Mysql target", Port: Target.Port},
		))
	}()

	//---

	emitter, err := debezium.NewMessagesEmitter(map[string]string{
		debeziumparameters.TopicPrefix:      "my_topic",
		debeziumparameters.AddOriginalTypes: "true",
		debeziumparameters.SourceType:       "mysql",
	}, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)
	receiver := debezium.NewReceiver(nil, nil)

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement)
	transfer.Src.(*mysql.MysqlSource).PlzNoHomo = true
	transfer.Src.(*mysql.MysqlSource).AllowDecimalAsFloat = true
	debeziumSerDeTransformer := helpers.NewSimpleTransformer(t, serde.MakeDebeziumSerDeUdfWithoutCheck(emitter, receiver), serde.AnyTablesUdf)
	helpers.AddTransformer(t, transfer, debeziumSerDeTransformer)
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	//---

	connParams, err := mysql.NewConnectionParams(Source.ToStorageParams())
	require.NoError(t, err)
	db, err := mysql.Connect(connParams, nil)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO customers3 (pk) VALUES (2);`)
	require.NoError(t, err)

	//---

	require.NoError(t, helpers.WaitEqualRowsCountDifferentSchemas(t,
		Source.Database, Target.Database, "customers3",
		helpers.GetSampleableStorageByModel(t, Source),
		helpers.GetSampleableStorageByModel(t, Target),
		60*time.Second))
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}
