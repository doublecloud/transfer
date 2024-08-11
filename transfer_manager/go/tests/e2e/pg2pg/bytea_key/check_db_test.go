package byteakey

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/coordinator"
	pgcommon "github.com/doublecloud/tross/transfer_manager/go/pkg/providers/postgres"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/postgres/pgrecipe"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/runtime/local"
	"github.com/doublecloud/tross/transfer_manager/go/tests/helpers"
	"github.com/stretchr/testify/require"
)

var (
	TransferType = abstract.TransferTypeIncrementOnly
	Source       = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("init_source"), pgrecipe.WithDBTables("public.test"))
	Target       = *pgrecipe.RecipeTarget(pgrecipe.WithInitDir("init_target"))
)

func init() {
	_ = os.Setenv("YC", "1")                                               // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestByteaKey(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)

	srcConn, err := pgcommon.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()
	dstConn, err := pgcommon.MakeConnPoolFromDst(&Target, logger.Log)
	require.NoError(t, err)
	defer dstConn.Close()

	_, err = srcConn.Exec(context.Background(), `SELECT pg_create_logical_replication_slot('testslot', 'wal2json')`)
	require.NoError(t, err)
	defer func() {
		_, err := srcConn.Exec(context.Background(), `SELECT pg_drop_replication_slot('testslot')`)
		require.NoError(t, err)
	}()

	_, err = srcConn.Exec(context.Background(), `INSERT INTO test VALUES ('\xdeadbeef', 'a')`)
	require.NoError(t, err)
	_, err = srcConn.Exec(context.Background(), `UPDATE test SET value = 'b'`)
	require.NoError(t, err)
	_, err = srcConn.Exec(context.Background(), `INSERT INTO test VALUES ('\xB16B00B5', 'b')`)
	require.NoError(t, err)

	worker := local.NewLocalWorker(coordinator.NewFakeClient(), transfer, helpers.EmptyRegistry(), logger.Log)
	worker.Start()
	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "test", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 60*time.Second))
	err = worker.Stop()
	require.NoError(t, err)

	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}
