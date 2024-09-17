package usertypes

import (
	"context"
	"os"
	"testing"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/pkg/abstract/model"
	pgcommon "github.com/doublecloud/transfer/pkg/providers/postgres"
	"github.com/doublecloud/transfer/pkg/providers/postgres/pgrecipe"
	"github.com/doublecloud/transfer/pkg/runtime/local"
	"github.com/doublecloud/transfer/tests/helpers"
	"github.com/stretchr/testify/require"
)

var (
	Source   = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("init_source"))
	Target   = *pgrecipe.RecipeTarget()
	ErrRetry = xerrors.NewSentinel("Retry")
)

func init() {
	_ = os.Setenv("YC", "1")                                                                            // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func loadSnapshot(t *testing.T) {
	Source.PreSteps.Constraint = true
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotOnly)

	_ = helpers.Activate(t, transfer)

	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}

func checkReplicationWorks(t *testing.T) {
	transfer := server.Transfer{
		ID:   "test_id",
		Src:  &Source,
		Dst:  &Target,
		Type: abstract.TransferTypeSnapshotAndIncrement,
	}

	srcConn, err := pgcommon.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()

	worker := local.NewLocalWorker(coordinator.NewFakeClient(), &transfer, helpers.EmptyRegistry(), logger.Log)
	worker.Start()
	defer worker.Stop()

	_, err = srcConn.Exec(context.Background(), `INSERT INTO testtable VALUES (2, 'choovuck', 'zhepa', 'EinScheissdreckWerdeIchTun', (2, '456')::udt, ARRAY [(3, 'foo1')::udt, (4, 'bar1')::udt])`)
	require.NoError(t, err)
	require.NoError(t, helpers.WaitStoragesSynced(t, Source, Target, 50, helpers.NewCompareStorageParams()))

	tag, err := srcConn.Exec(context.Background(), `UPDATE testtable SET fancy = 'zhopa', deuch = 'DuGehstMirAufDieEier', udt = (3, '789')::udt, udt_arr = ARRAY [(5, 'foo2')::udt, (6, 'bar2')::udt] where id = 2`)
	require.NoError(t, err)
	require.EqualValues(t, tag.RowsAffected(), 1)
	require.NoError(t, helpers.WaitStoragesSynced(t, Source, Target, 50, helpers.NewCompareStorageParams()))
}

func TestUserTypes(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	loadSnapshot(t)
	// loadSnapshot always assigns true to CopyUpload flag which is used by sinker.
	// In order for replication to work we must set CopyUpload value back to false.
	Target.CopyUpload = false
	checkReplicationWorks(t)
}
