package snapshot

import (
	"context"
	"os"
	"testing"

	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	client2 "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/clickhouse/model"
	chrecipe "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/clickhouse/recipe"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/postgres"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/postgres/pgrecipe"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/worker/tasks"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/stretchr/testify/require"
)

var (
	databaseName = "public"
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump/pg"))
	Target       = *chrecipe.MustTarget(chrecipe.WithInitDir("dump/ch"), chrecipe.WithDatabase(databaseName))
)

func init() {
	_ = os.Setenv("YC", "1")                                              // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func testSnapshot(t *testing.T, source *postgres.PgSource, target model.ChDestination) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: source.Port},
			helpers.LabeledPort{Label: "CH target Native", Port: target.NativePort},
			helpers.LabeledPort{Label: "CH target HTTP", Port: target.HTTPPort},
		))
	}()

	transfer := helpers.MakeTransfer(helpers.TransferID, source, &target, TransferType)
	tables, err := tasks.ObtainAllSrcTables(transfer, helpers.EmptyRegistry())
	require.NoError(t, err)
	snapshotLoader := tasks.NewSnapshotLoader(client2.NewFakeClient(), "test-operation", transfer, helpers.EmptyRegistry())
	err = snapshotLoader.UploadTables(context.Background(), tables.ConvertToTableDescriptions(), true)
	require.Error(t, err)
	require.True(t, abstract.IsFatal(err))
}

func TestSnapshot(t *testing.T) {
	target := Target

	testSnapshot(t, Source, target)
}
