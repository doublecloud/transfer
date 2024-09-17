package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/debezium"
	debeziumparameters "github.com/doublecloud/transfer/pkg/debezium/parameters"
	pgcommon "github.com/doublecloud/transfer/pkg/providers/postgres"
	"github.com/doublecloud/transfer/pkg/providers/postgres/pgrecipe"
	"github.com/doublecloud/transfer/tests/helpers"
	"github.com/doublecloud/transfer/tests/helpers/serde"
	"github.com/stretchr/testify/require"
)

var (
	Source = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("init_source"))
	Target = *pgrecipe.RecipeTarget()
)

var insertStmt = `
INSERT INTO public.user_table VALUES (5,123,ARRAY ['VALUE_THREE','VALUE_ONE']::my_enum_type[]);
INSERT INTO public.user_table VALUES (6,321,null);

-- TODO: for this cases we need to update wal2json
-- INSERT INTO public.double_precision_values VALUES (4,'-Infinity');
-- INSERT INTO public.double_precision_values VALUES (5,'Infinity');
-- INSERT INTO public.double_precision_values VALUES (6,'NaN');
`

func init() {
	_ = os.Setenv("YC", "1")                                                                            // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestSnapshotAndIncrement(t *testing.T) {
	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "PG source", Port: Source.Port},
	))
	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "PG source", Port: Source.Port},
		helpers.LabeledPort{Label: "PG target", Port: Target.Port},
	))

	//---

	emitter, err := debezium.NewMessagesEmitter(map[string]string{
		debeziumparameters.DatabaseDBName:   "public",
		debeziumparameters.TopicPrefix:      "my_topic",
		debeziumparameters.AddOriginalTypes: "true",
		debeziumparameters.SourceType:       "pg",
	}, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)
	receiver := debezium.NewReceiver(nil, nil)

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement)
	transfer.Src.(*pgcommon.PgSource).NoHomo = true

	debeziumSerDeTransformer := helpers.NewSimpleTransformer(t, serde.MakeDebeziumSerDeUdfWithCheck(emitter, receiver), serde.AnyTablesUdf)
	helpers.AddTransformer(t, transfer, debeziumSerDeTransformer)
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	//---

	srcConn, err := pgcommon.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()

	_, err = srcConn.Exec(context.Background(), insertStmt)
	require.NoError(t, err)

	//---

	require.NoError(t, helpers.WaitDestinationEqualRowsCount("public", "user_table", helpers.GetSampleableStorageByModel(t, Target), 60*time.Second, 6))
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams().WithPriorityComparators(helpers.PgDebeziumIgnoreTemporalAccuracyForArraysComparator)))
	require.Equal(t, 6, serde.CountOfProcessedMessage)
}
