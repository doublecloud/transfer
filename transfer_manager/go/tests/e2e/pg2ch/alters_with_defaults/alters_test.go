package alters

import (
	"context"
	"testing"
	"time"

	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/clickhouse/model"
	chrecipe "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/clickhouse/recipe"
	pgcommon "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/postgres"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/postgres/pgrecipe"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/e2e/pg2ch"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
)

var (
	databaseName = "public"
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump/pg"), pgrecipe.WithPrefix(""))
	Target       = *chrecipe.MustTarget(chrecipe.WithInitDir("dump/ch"), chrecipe.WithDatabase(databaseName))
)

func init() {
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestAlter(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "CH target", Port: Target.NativePort},
		))
	}()

	connConfig, err := pgcommon.MakeConnConfigFromSrc(logger.Log, &Source)
	require.NoError(t, err)
	conn, err := pgcommon.NewPgConnPool(connConfig, logger.Log)
	require.NoError(t, err)

	//------------------------------------------------------------------------------------
	// start worker

	Target.ProtocolUnspecified = true
	Target.MigrationOptions = &model.ChSinkMigrationOptions{
		AddNewColumns: true,
	}
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)
	transfer.DataObjects = &server.DataObjects{IncludeObjects: []string{"public.__test"}}
	var terminateErr error
	localWorker := helpers.Activate(t, transfer, func(err error) {
		terminateErr = err
	})
	defer localWorker.Close(t)

	t.Run("ADD COLUMN with defaults", func(t *testing.T) {
		// force INSERTs with different schemas to be pushed with one ApplyChangeItems call
		err := conn.BeginFunc(context.Background(), func(tx pgx.Tx) error {
			rows, err := tx.Query(context.Background(), "INSERT INTO __test (id, val1, val2) VALUES (3, 3, 'e')")
			require.NoError(t, err)
			rows.Close()

			rows, err = tx.Query(context.Background(), "ALTER TABLE __test ADD COLUMN new_val1 TEXT DEFAULT 'test default value'")
			require.NoError(t, err)
			rows.Close()

			rows, err = tx.Query(context.Background(), "ALTER TABLE __test ADD COLUMN new_val2 INTEGER DEFAULT 1")
			require.NoError(t, err)
			rows.Close()

			rows, err = tx.Query(context.Background(), "INSERT INTO __test (id, val1, val2, new_val1, new_val2) VALUES (4, 4, 'f', '4', 4)")
			require.NoError(t, err)
			rows.Close()
			return nil
		})
		require.NoError(t, err)

		//------------------------------------------------------------------------------------
		// wait & compare

		require.NoError(t, helpers.WaitEqualRowsCount(t, databaseName, "__test", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 60*time.Second))
		require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams().WithEqualDataTypes(pg2ch.PG2CHDataTypesComparator)))
	})

	t.Run("ADD COLUMN with complex defaults", func(t *testing.T) {
		// force INSERTs with different schemas to be pushed with one ApplyChangeItems call
		err := conn.BeginFunc(context.Background(), func(tx pgx.Tx) error {
			rows, err := tx.Query(context.Background(), "INSERT INTO __test (id, val1, val2) VALUES (5, 5, 'e')")
			require.NoError(t, err)
			rows.Close()

			rows, err = tx.Query(context.Background(), "ALTER TABLE __test ADD COLUMN new_val3 TEXT DEFAULT pg_size_pretty(EXTRACT(EPOCH from  now())::bigint)")
			require.NoError(t, err)
			rows.Close()

			rows, err = tx.Query(context.Background(), "INSERT INTO __test (id, val1, val2, new_val1, new_val2) VALUES (6, 6, 'f', '6', 6)")
			require.NoError(t, err)
			rows.Close()
			return nil
		})
		require.NoError(t, err)

		//------------------------------------------------------------------------------------
		// wait & compare

		st := time.Now()
		for time.Since(st) < time.Minute {
			time.Sleep(time.Second)
			if terminateErr != nil {
				break
			}
		}
		require.Error(t, terminateErr)
		require.True(t, abstract.IsFatal(terminateErr))
	})
}
