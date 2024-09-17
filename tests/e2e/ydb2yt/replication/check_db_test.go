package main

import (
	"os"
	"testing"
	"time"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/pkg/abstract"
	server "github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/providers/ydb"
	ytcommon "github.com/doublecloud/transfer/pkg/providers/yt"
	"github.com/doublecloud/transfer/tests/helpers"
	"github.com/stretchr/testify/require"
)

func TestSnapshotAndReplication(t *testing.T) {
	currTableName := "test_table"

	source := &ydb.YdbSource{
		Token:              server.SecretString(os.Getenv("YDB_TOKEN")),
		Database:           helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             []string{currTableName},
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           false,
		ServiceAccountID:   "",
		ChangeFeedMode:     ydb.ChangeFeedModeUpdates,
	}
	target := ytcommon.NewYtDestinationV1(ytcommon.YtDestination{
		Path:                     "//home/cdc/test/pg2yt_e2e",
		Cluster:                  os.Getenv("YT_PROXY"),
		CellBundle:               "default",
		PrimaryMedium:            "default",
		UseStaticTableOnSnapshot: false, // TM-4444
	})
	transferType := abstract.TransferTypeSnapshotAndIncrement
	helpers.InitSrcDst(helpers.TransferID, source, target, transferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable

	//---

	Target := &ydb.YdbDestination{
		Database: source.Database,
		Token:    source.Token,
		Instance: source.Instance,
	}
	Target.WithDefaults()
	srcSink, err := ydb.NewSinker(logger.Log, Target, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	// insert one rec - for snapshot uploading

	currChangeItem := helpers.YDBStmtInsert(t, currTableName, 1)
	require.NoError(t, srcSink.Push([]abstract.ChangeItem{*currChangeItem}))

	// start snapshot & replication

	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, transferType)
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	helpers.CheckRowsCount(t, target, "", currTableName, 1)

	// insert two more records - it's three of them now

	require.NoError(t, srcSink.Push([]abstract.ChangeItem{
		*helpers.YDBStmtInsert(t, currTableName, 2),
		*helpers.YDBStmtInsert(t, currTableName, 3),
	}))

	// update 2nd rec

	require.NoError(t, srcSink.Push([]abstract.ChangeItem{
		*helpers.YDBStmtUpdate(t, currTableName, 2, 666),
	}))

	// update 3rd rec by TOAST

	require.NoError(t, srcSink.Push([]abstract.ChangeItem{
		*helpers.YDBStmtUpdateTOAST(t, currTableName, 3, 777),
	}))

	// delete 1st rec

	require.NoError(t, srcSink.Push([]abstract.ChangeItem{
		*helpers.YDBStmtDelete(t, currTableName, 1),
	}))

	// check

	require.NoError(t, helpers.WaitDestinationEqualRowsCount("", currTableName, helpers.GetSampleableStorageByModel(t, target), 60*time.Second, 2))
}
