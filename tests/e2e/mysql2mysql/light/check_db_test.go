package light

import (
	"context"
	"os"
	"testing"

	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/coordinator"
	"github.com/doublecloud/transfer/pkg/providers/mysql"
	"github.com/doublecloud/transfer/pkg/worker/tasks"
	"github.com/doublecloud/transfer/tests/helpers"
	"github.com/stretchr/testify/require"
)

var (
	TransferType = abstract.TransferTypeSnapshotOnly
	Source       = *helpers.RecipeMysqlSource()
	Target       = *helpers.RecipeMysqlTarget()
)

func init() {
	_ = os.Setenv("YC", "1")                                               // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mysql source", Port: Source.Port},
			helpers.LabeledPort{Label: "Mysql target", Port: Target.Port},
		))
	}()

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Existence", Existence)
		t.Run("Snapshot", Snapshot)
	})
}

func Existence(t *testing.T) {
	_, err := mysql.NewStorage(Source.ToStorageParams())
	require.NoError(t, err)
	_, err = mysql.NewStorage(Target.ToStorageParams())
	require.NoError(t, err)
}

func Snapshot(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)

	require.NoError(t, tasks.ActivateDelivery(context.TODO(), nil, coordinator.NewFakeClient(), *transfer, helpers.EmptyRegistry()))
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}
