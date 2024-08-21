package snapshot

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	pgcommon "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/postgres"
	ytcommon "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/yt"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/worker/tasks"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	yt_helpers "github.com/doublecloud/transfer/transfer_manager/go/tests/helpers/yt"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/ypath"
	ytMain "go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

const ytPath = "//home/cdc/test/pg2yt_e2e"

var (
	Source = pgcommon.PgSource{
		ClusterID: os.Getenv("PG_CLUSTER_ID"),
		Hosts:     []string{"localhost"},
		User:      os.Getenv("PG_LOCAL_USER"),
		Password:  server.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:  os.Getenv("PG_LOCAL_DATABASE"),
		Port:      helpers.GetIntFromEnv("PG_LOCAL_PORT"),
		DBTables:  []string{"public.__test"},
	}
	Target = yt_helpers.RecipeYtTarget(ytPath)
)

const cursorField = "id"
const cursorValue = "5"

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
}

func TestMain(m *testing.M) {
	ytcommon.InitExe()
	os.Exit(m.Run())
}

func TestGroup(t *testing.T) {
	targetPort, err := helpers.GetPortFromStr(Target.Cluster())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	ctx := context.Background()
	_, err = ytEnv.YT.CreateNode(ctx, ypath.Path(ytPath), ytMain.NodeMap, &ytMain.CreateNodeOptions{Recursive: true})
	defer func() {
		err := ytEnv.YT.RemoveNode(ctx, ypath.Path(ytPath), &ytMain.RemoveNodeOptions{Recursive: true})
		require.NoError(t, err)
	}()
	require.NoError(t, err)

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Snapshot", Snapshot)
	})
}

func Snapshot(t *testing.T) {
	Source.PreSteps.Constraint = true
	transfer := helpers.MakeTransferForIncrementalSnapshot(helpers.TransferID, &Source, Target, abstract.TransferTypeSnapshotOnly,
		"public", "__test", cursorField, cursorValue, 15)
	transfer.Runtime = new(abstract.YtRuntime)

	fakeClient := coordinator.NewStatefulFakeClient()

	//------------------------------------------------------------------------------
	removeAddedData(t)

	tables, err := tasks.ObtainAllSrcTables(transfer, helpers.EmptyRegistry())
	require.NoError(t, err)
	snapshotLoader := tasks.NewSnapshotLoader(fakeClient, "test-operation", transfer, helpers.EmptyRegistry())
	err = snapshotLoader.UploadTables(context.Background(), tables.ConvertToTableDescriptions(), true)
	require.NoError(t, err)

	conn, err := pgcommon.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	defer conn.Close()

	expectedYtRows := getExpectedRowsCount(t, conn, 0)
	storage := helpers.GetSampleableStorageByModel(t, Target.LegacyModel())
	require.NoError(t, helpers.WaitDestinationEqualRowsCount("public", "__test",
		storage, 60*time.Second, expectedYtRows), "Wrong row number after first snapshot round!")

	addSomeData(t, conn)
	done := addSomeConcurrentDataAsyncWithDelay(t, 15, conn)

	err = snapshotLoader.UploadTables(context.Background(), tables.ConvertToTableDescriptions(), true)
	require.NoError(t, err)
	logger.Log.Infof("Done loading data %v", <-done)

	expectedYtRows = getExpectedRowsCount(t, conn, 1)
	require.NoError(t, helpers.WaitDestinationEqualRowsCount("public", "__test",
		storage, 60*time.Second, expectedYtRows), "Wrong row number after full increment round!")

	ids := readIdsFromTarget(t, storage)

	require.Contains(t, ids, int64(16), "Id 16 should be loaded!!")
	require.Contains(t, ids, int64(18), "Id 18 should be loaded!!")
	require.NotContains(t, ids, int64(20), "Id 20 should not be loaded during current increment cycle!")

	err = snapshotLoader.UploadTables(context.Background(), tables.ConvertToTableDescriptions(), true)
	require.NoError(t, err)

	expectedYtRows = getExpectedRowsCount(t, conn, 0)
	require.NoError(t, helpers.WaitDestinationEqualRowsCount("public", "__test",
		storage, 60*time.Second, expectedYtRows), "Wrong row number after full increment round!")

	ids = readIdsFromTarget(t, storage)
	require.Contains(t, ids, int64(20), "Id 20 should be loaded during last increment cycle!")
	removeAddedData(t)
}

func readIdsFromTarget(t *testing.T, storage abstract.SampleableStorage) []int64 {
	ids := make([]int64, 0)

	require.NoError(t, storage.LoadTable(context.Background(), abstract.TableDescription{
		Name:   "__test",
		Schema: "",
		Filter: "",
		EtaRow: 0,
		Offset: 0,
	}, func(items []abstract.ChangeItem) error {
		for _, row := range items {
			if !row.IsRowEvent() {
				continue
			}
			id := row.ColumnNameIndex("id")
			ids = append(ids, row.ColumnValues[id].(int64))
		}
		return nil
	}))
	return ids
}

func getExpectedRowsCount(t *testing.T, conn *pgxpool.Pool, exclude uint64) uint64 {
	var cnt uint64
	err := conn.QueryRow(context.Background(), "select count(*) from __test where id > 5").Scan(&cnt)
	require.NoError(t, err, "Cannot get rows count")

	return cnt - exclude //should not get last inserted row
}

func removeAddedData(t *testing.T) {
	conn, err := pgcommon.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(), "delete from __test where id >= 14")
	require.NoError(t, err)
}

func addSomeData(t *testing.T, conn *pgxpool.Pool) {
	logger.Log.Info("Will add some data after snapshot...")
	_, err := conn.Exec(context.Background(), "insert into __test (str, id, da, i) values ('qqq', 14, '1999-09-16', 1)")
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(), `insert into __test (str, id, da, i) values
                                                      ('www', 15, '1999-09-16', 1),
                                                      ('eee', 17, '1999-09-16', 1),
                                                      ('rrr', 19, '1999-09-16', 1) `)
	require.NoError(t, err)
}

func addSomeConcurrentDataAsyncWithDelay(t *testing.T, delay int64, conn *pgxpool.Pool) chan bool {
	r := make(chan bool)
	go func() {
		logger.Log.Info("Will add some data asynchronously...")
		logger.Log.Info("Start adding some late concurrent data with sleep")
		query := "" +
			"begin;" +
			"insert into __test (str, id, da, i) values" +
			"    ('late data', 18, '2022-09-16', 1)," +
			"    ('late data 2', 16, '2022-10-16', 1)," +
			"    ('late data 3', 20, '2022-09-17', 1);" +
			"SELECT pg_sleep(" + strconv.FormatInt(delay-5, 10) + ");" +
			"commit;"
		_, err := conn.Exec(context.Background(), query)
		require.NoError(t, err)
		logger.Log.Info("Adding late data done!")
		r <- true
	}()
	return r
}
