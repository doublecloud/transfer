package replicationwithoutpk

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/pkg/abstract"
	pgcommon "github.com/doublecloud/transfer/pkg/providers/postgres"
	"github.com/doublecloud/transfer/pkg/providers/postgres/pgrecipe"
	"github.com/doublecloud/transfer/tests/helpers"
	"github.com/stretchr/testify/require"
)

const tableName = "public.__test"

var (
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("dump"))
	Target       = pgrecipe.RecipeTarget(pgrecipe.WithPrefix("DB0_"))
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

func TestUpdatesWithoutSnapshot(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	helpers.InitSrcDst(helpers.TransferID, Source, Target, TransferType)

	transfer := helpers.MakeTransfer(helpers.TransferID, Source, Target, TransferType)

	srcConn, err := pgcommon.MakeConnPoolFromSrc(Source, logger.Log)
	require.NoError(t, err)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	_, err = srcConn.Exec(context.Background(), fmt.Sprintf("INSERT INTO %s VALUES (1,6);", tableName))
	require.NoError(t, err)

	_, err = srcConn.Exec(context.Background(), fmt.Sprintf("UPDATE %s SET a=1;", tableName))
	require.NoError(t, err)

	_, err = srcConn.Exec(context.Background(), fmt.Sprintf("INSERT INTO %s VALUES (7,8);", tableName))
	require.NoError(t, err)

	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "__test", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 60*time.Second))
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}
