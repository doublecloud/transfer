package usertypes

import (
	"os"
	"testing"

	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/providers/postgres/pgrecipe"
	"github.com/doublecloud/transfer/tests/helpers"
	"github.com/stretchr/testify/require"
)

var (
	Source = pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump"))
	Target = pgrecipe.RecipeTarget()
)

func init() {
	_ = os.Setenv("YC", "1")                                                                          // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, Source, Target, abstract.TransferTypeSnapshotAndIncrement) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestSnapshot(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	transfer := helpers.MakeTransfer(helpers.TransferID, Source, Target, abstract.TransferTypeSnapshotOnly)
	transfer.TypeSystemVersion = 1

	_ = helpers.Activate(t, transfer)

	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}
