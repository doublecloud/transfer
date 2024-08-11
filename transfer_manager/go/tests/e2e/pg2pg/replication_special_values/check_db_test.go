package replicationview

import (
	"context"
	"testing"
	"time"

	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/postgres"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/postgres/pgrecipe"
	"github.com/doublecloud/tross/transfer_manager/go/tests/helpers"
	"github.com/stretchr/testify/require"
)

func TestReplicationNullInJSON(t *testing.T) {
	Source := pgrecipe.RecipeSource(pgrecipe.WithInitDir("init_source"))
	Target := pgrecipe.RecipeTarget()
	transferType := abstract.TransferTypeSnapshotAndIncrement

	helpers.InitSrcDst(helpers.TransferID, Source, Target, transferType)

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	transfer := helpers.MakeTransfer(helpers.TransferID, Source, Target, transferType)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	srcConn, err := postgres.MakeConnPoolFromSrc(Source, logger.Log)
	require.NoError(t, err)

	_, err = srcConn.Exec(context.Background(), `INSERT INTO rsv_null_in_json(i, j, jb) VALUES (101, 'null', 'null'), (102, '"null"', '"null"')`)
	require.NoError(t, err)

	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "rsv_null_in_json", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 60*time.Second))
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}
