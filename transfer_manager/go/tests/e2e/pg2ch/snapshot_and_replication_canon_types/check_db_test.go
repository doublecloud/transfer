package replication

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	chrecipe "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/clickhouse/recipe"
	pgcommon "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/postgres"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/postgres/pgrecipe"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/canon/postgres"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/e2e/pg2ch"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/stretchr/testify/require"
)

var (
	databaseName = "public"
	TransferType = abstract.TransferTypeSnapshotAndIncrement
)

func TestSnapshotAndIncrement(t *testing.T) {
	_ = os.Setenv("YC", "1") // to not go to vanga

	Source := pgrecipe.RecipeSource(pgrecipe.WithPrefix(""))
	Target := chrecipe.MustTarget(chrecipe.WithInitDir("dump/ch"), chrecipe.WithDatabase(databaseName))
	_ = os.Setenv("YC", "1")                                             // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, Source, Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "CH target", Port: Target.NativePort},
		))
	}()

	tableCase := func(tableName string) func(t *testing.T) {
		return func(t *testing.T) {
			tid, err := abstract.ParseTableID(tableName)
			require.NoError(t, err)
			conn, err := pgcommon.MakeConnPoolFromSrc(Source, logger.Log)
			require.NoError(t, err)
			_, err = conn.Exec(context.Background(), fmt.Sprintf(`drop table if exists %s`, tableName))
			require.NoError(t, err)
			_, err = conn.Exec(context.Background(), postgres.TableSQLs[tableName])
			require.NoError(t, err)

			transfer := helpers.MakeTransfer(
				tableName,
				Source,
				Target,
				abstract.TransferTypeSnapshotAndIncrement,
			)
			transfer.DataObjects = &server.DataObjects{IncludeObjects: []string{tableName}}
			worker := helpers.Activate(t, transfer)

			conn, err = pgcommon.MakeConnPoolFromSrc(Source, logger.Log)
			require.NoError(t, err)
			_, err = conn.Exec(context.Background(), postgres.TableSQLs[tableName])
			require.NoError(t, err)
			require.NoError(t, helpers.WaitEqualRowsCount(t, databaseName, tid.Name, helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 60*time.Second))
			require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams().WithEqualDataTypes(pg2ch.PG2CHDataTypesComparator).WithPriorityComparators(pg2ch.ValueComparator)))
			defer worker.Close(t)
		}
	}
	// t.Run("array_types", tableCase("public.array_types"))
	t.Run("date_types", tableCase("public.date_types"))
	t.Run("geom_types", tableCase("public.geom_types"))
	t.Run("numeric_types", tableCase("public.numeric_types"))
	t.Run("text_types", tableCase("public.text_types"))
	// t.Run("wtf_types", tableCase("public.wtf_types"))
}
