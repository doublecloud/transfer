package light

import (
	"os"
	"testing"

	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/postgres/pgrecipe"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/stretchr/testify/require"
)

var (
	TransferType = abstract.TransferTypeSnapshotOnly

	Source = *pgrecipe.RecipeSource()
	Target = *helpers.RecipeMysqlTarget()
)

func init() {
	_ = os.Setenv("YC", "1")                                               // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "pg source", Port: Source.Port},
			helpers.LabeledPort{Label: "mysql target", Port: Target.Port},
		))
	}()

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Snapshot", Snapshot)
	})
}

func Snapshot(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)
	_ = helpers.Activate(t, transfer)
	helpers.CheckRowsCount(t, Target, Target.Database, "__test", 16)
}
