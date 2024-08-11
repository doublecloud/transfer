package alltypes

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
	pgStorage "github.com/doublecloud/tross/transfer_manager/go/pkg/providers/postgres"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/postgres/pgrecipe"
	"github.com/doublecloud/tross/transfer_manager/go/tests/canon/postgres"
	"github.com/doublecloud/tross/transfer_manager/go/tests/helpers"
	"github.com/stretchr/testify/require"
)

func TestAllDataTypes(t *testing.T) {
	Source := pgrecipe.RecipeSource(pgrecipe.WithPrefix(""))
	Source.WithDefaults()
	Target := pgrecipe.RecipeTarget(pgrecipe.WithPrefix("DB0_"))
	conn, err := pgStorage.MakeConnPoolFromDst(Target, logger.Log)
	require.NoError(t, err)
	// TODO: Allow to optionally transit extensions as part of transfer
	_, err = conn.Exec(context.Background(), `
create extension if not exists hstore;
create extension if not exists ltree;
create extension if not exists citext;
`)
	require.NoError(t, err)

	helpers.InitSrcDst(helpers.TransferID, Source, Target, abstract.TransferTypeSnapshotAndIncrement)

	tableCase := func(tableName string) func(t *testing.T) {
		return func(t *testing.T) {
			t.Run("initial data", func(t *testing.T) {
				conn, err := pgStorage.MakeConnPoolFromSrc(Source, logger.Log)
				require.NoError(t, err)
				_, err = conn.Exec(context.Background(), postgres.TableSQLs[tableName])
				require.NoError(t, err)
			})

			Source.DBTables = []string{tableName}
			transfer := helpers.MakeTransfer(
				t.Name(),
				Source,
				Target,
				abstract.TransferTypeSnapshotAndIncrement,
			)
			transfer.DataObjects = &server.DataObjects{IncludeObjects: []string{tableName}}
			worker := helpers.Activate(t, transfer)

			conn, err := pgStorage.MakeConnPoolFromSrc(Source, logger.Log)
			require.NoError(t, err)
			_, err = conn.Exec(context.Background(), postgres.TableSQLs[tableName])
			require.NoError(t, err)
			srcStorage, err := pgStorage.NewStorage(Source.ToStorageParams(nil))
			require.NoError(t, err)
			dstStorage, err := pgStorage.NewStorage(Target.ToStorageParams())
			require.NoError(t, err)
			tid, err := abstract.ParseTableID(tableName)
			require.NoError(t, err)
			require.NoError(t, helpers.WaitEqualRowsCount(t, tid.Namespace, tid.Name, srcStorage, dstStorage, time.Second*30))
			worker.Close(t)
			hashQuery := fmt.Sprintf(`
SELECT md5(array_agg(md5((t.*)::varchar))::varchar)
  FROM (
        SELECT *
          FROM %s
         ORDER BY 1
       ) AS t
;
`, tableName)
			var srcHash string
			require.NoError(t, srcStorage.Conn.QueryRow(context.Background(), hashQuery).Scan(&srcHash))
			var dstHash string
			require.NoError(t, srcStorage.Conn.QueryRow(context.Background(), hashQuery).Scan(&dstHash))
			require.Equal(t, srcHash, dstHash)
		}
	}
	t.Run("array_types", tableCase("public.array_types"))
	t.Run("date_types", tableCase("public.date_types"))
	t.Run("geom_types", tableCase("public.geom_types"))
	t.Run("numeric_types", tableCase("public.numeric_types"))
	t.Run("text_types", tableCase("public.text_types"))
	t.Run("wtf_types", tableCase("public.wtf_types"))
}
