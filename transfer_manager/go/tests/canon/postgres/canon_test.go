package postgres

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/postgres"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/postgres/pgrecipe"
	"github.com/doublecloud/tross/transfer_manager/go/tests/canon/validator"
	"github.com/doublecloud/tross/transfer_manager/go/tests/helpers"
	"github.com/doublecloud/tross/transfer_manager/go/tests/tcrecipes"
	"github.com/stretchr/testify/require"
)

func TestCanonSource(t *testing.T) {
	if tcrecipes.Enabled() {
		_ = pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("dump"))
	}
	_ = os.Setenv("YC", "1") // to not go to vanga
	srcPort := helpers.GetIntFromEnv("PG_LOCAL_PORT")
	Source := &postgres.PgSource{
		ClusterID: os.Getenv("PG_CLUSTER_ID"),
		Hosts:     []string{"localhost"},
		User:      os.Getenv("PG_LOCAL_USER"),
		Password:  server.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:  os.Getenv("PG_LOCAL_DATABASE"),
		Port:      srcPort,
		SlotID:    "test_slot_id",
	}
	Source.WithDefaults()
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
		))
	}()

	tableCase := func(tableName string) func(t *testing.T) {
		return func(t *testing.T) {
			conn, err := postgres.MakeConnPoolFromSrc(Source, logger.Log)
			require.NoError(t, err)
			_, err = conn.Exec(context.Background(), fmt.Sprintf(`drop table if exists %s`, tableName))
			require.NoError(t, err)
			_, err = conn.Exec(context.Background(), TableSQLs[tableName])
			require.NoError(t, err)

			counterStorage, counterSinkFactory := validator.NewCounter()
			transfer := helpers.MakeTransfer(
				tableName,
				Source,
				&server.MockDestination{
					SinkerFactory: validator.New(
						server.IsStrictSource(Source),
						validator.InitDone(t),
						validator.Canonizator(t),
						validator.TypesystemChecker(postgres.ProviderType, func(colSchema abstract.ColSchema) string {
							return postgres.ClearOriginalType(colSchema.OriginalType)
						}),
						counterSinkFactory,
					),
					Cleanup: server.DisabledCleanup,
				},
				abstract.TransferTypeSnapshotAndIncrement,
			)
			transfer.DataObjects = &server.DataObjects{IncludeObjects: []string{tableName}}
			worker := helpers.Activate(t, transfer)

			conn, err = postgres.MakeConnPoolFromSrc(Source, logger.Log)
			require.NoError(t, err)
			_, err = conn.Exec(context.Background(), TableSQLs[tableName])
			require.NoError(t, err)
			srcStorage, err := postgres.NewStorage(Source.ToStorageParams(transfer))
			require.NoError(t, err)

			require.NoError(t, helpers.WaitEqualRowsCount(t, strings.Split(tableName, ".")[0], strings.Split(tableName, ".")[1], srcStorage, counterStorage, time.Second*60))
			defer worker.Close(t)
		}
	}
	t.Run("array_types", tableCase("public.array_types"))
	t.Run("date_types", tableCase("public.date_types"))
	t.Run("geom_types", tableCase("public.geom_types"))
	t.Run("numeric_types", tableCase("public.numeric_types"))
	t.Run("text_types", tableCase("public.text_types"))
	t.Run("wtf_types", tableCase("public.wtf_types"))
}
