package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/debezium"
	debeziumparameters "github.com/doublecloud/tross/transfer_manager/go/pkg/debezium/parameters"
	pgcommon "github.com/doublecloud/tross/transfer_manager/go/pkg/providers/postgres"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/postgres/pgrecipe"
	"github.com/doublecloud/tross/transfer_manager/go/tests/helpers"
	"github.com/doublecloud/tross/transfer_manager/go/tests/helpers/serde"
	"github.com/stretchr/testify/require"
)

var (
	Source = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("init_source"))
	Target = *pgrecipe.RecipeTarget()
)

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

	_, err = srcConn.Exec(context.Background(), `
		INSERT INTO public.my_table VALUES
		(
			2,

			-32768, -- t_smallint
			-2147483648, -- t_integer
			-9223372036854775808, -- t_bigint

		    -0.01,

		    '2022-08-28 19:49:47.090000Z' -- TIMESTAMPTZ
		);

		INSERT INTO public.my_table VALUES
		(
			3,

			32767, -- t_smallint
			2147483647, -- t_integer
			9223372036854775807, -- t_bigint

		    0.01,

		    '2022-08-28 19:49:47.090000Z' -- TIMESTAMPTZ
		);
    `)
	require.NoError(t, err)

	//---

	require.NoError(t, helpers.WaitDestinationEqualRowsCount("public", "my_table", helpers.GetSampleableStorageByModel(t, Target), 60*time.Second, 4))
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}
