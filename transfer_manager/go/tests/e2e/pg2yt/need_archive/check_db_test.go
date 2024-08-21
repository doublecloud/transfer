package replication

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/postgres"
	ytcommon "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/yt"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	ytMain "go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

var (
	srcPort = helpers.GetIntFromEnv("PG_LOCAL_PORT")
	Source  = postgres.PgSource{
		ClusterID: os.Getenv("PG_CLUSTER_ID"),
		Hosts:     []string{"localhost"},
		User:      os.Getenv("PG_LOCAL_USER"),
		Password:  server.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:  os.Getenv("PG_LOCAL_DATABASE"),
		Port:      srcPort,
		DBTables:  []string{"public.__test"},
		SlotID:    "test_slot_id",
	}
	Target = ytcommon.NewYtDestinationV1(ytcommon.YtDestination{
		Path:          "//home/cdc/test/pg2yt_e2e_replication",
		Cluster:       os.Getenv("YT_PROXY"),
		CellBundle:    "default",
		PrimaryMedium: "default",
		NeedArchive:   true,
	})
)

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

	ctx := context.Background()

	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	_, err = ytEnv.YT.CreateNode(ctx, ypath.Path("//home/cdc/test/pg2yt_e2e_replication"), ytMain.NodeMap, &ytMain.CreateNodeOptions{Recursive: true})
	defer func() {
		err := ytEnv.YT.RemoveNode(ctx, ypath.Path("//home/cdc/test/pg2yt_e2e_replication"), &ytMain.RemoveNodeOptions{Recursive: true})
		require.NoError(t, err)
	}()
	require.NoError(t, err)

	t.Run("Load", Load)
}

func Load(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, Target, abstract.TransferTypeSnapshotAndIncrement)
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	//------------------------------------------------------------------------------

	conn, err := postgres.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)

	_, err = conn.Exec(context.Background(), "alter table __test drop column astr")
	require.NoError(t, err)

	_, err = conn.Exec(context.Background(), "insert into __test (id, bstr, cstr) values (4, 'bstr4', 'cstr4'), (5, 'bstr5', 'cstr5')")
	require.NoError(t, err)

	_, err = conn.Exec(context.Background(), "delete from __test where id = -1")
	require.NoError(t, err)

	//------------------------------------------------------------------------------
	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	tablePath := ypath.Path(Target.Path()).Child("__test")
	waitForRows(t, ytEnv.YT, []ypath.Path{tablePath}, 5)

	archiveTablePath := ypath.Path(Target.Path()).Child("__test_archive")
	waitForRows(t, ytEnv.YT, []ypath.Path{archiveTablePath}, 1)

	var unparsedSchema schema.Schema
	require.NoError(t, ytEnv.YT.GetNode(context.Background(), archiveTablePath.Attr("schema"), &unparsedSchema, nil))
	require.True(t, schemaContainsColumn(unparsedSchema, "astr"))
}

func schemaContainsColumn(sch schema.Schema, colName string) bool {
	for _, c := range sch.Columns {
		if c.Name == colName {
			return true
		}
	}
	return false
}

func closeReader(reader ytMain.TableReader) {
	err := reader.Close()
	if err != nil {
		logger.Log.Warn("Could not close table reader")
	}
}

func checkRowCount(client ytMain.Client, tablePath ypath.Path, rowsNumber int) (bool, error) {
	reader, err := client.SelectRows(context.Background(), fmt.Sprintf("SUM(1) AS row_count FROM [%s] GROUP BY 1", tablePath), &ytMain.SelectRowsOptions{})
	if err != nil {
		return false, err
	}
	defer closeReader(reader)

	var result map[string]int
	if !reader.Next() {
		return false, err
	}
	err = reader.Scan(&result)
	if err != nil {
		return false, err
	}
	logger.Log.Infof("check row count for table %v: %v rows in table, wait for %v rows", tablePath, result["row_count"], rowsNumber)
	if result["row_count"] == rowsNumber {
		return true, nil
	}

	return false, nil
}

func waitForRows(t *testing.T, client ytMain.Client, tablePaths []ypath.Path, rowsNumber int) {
	finished := make([]bool, len(tablePaths))

	for {
		isNotFinishedAll := false

		for i, tablePath := range tablePaths {
			if !finished[i] {
				ok, err := checkRowCount(client, tablePath, rowsNumber)
				require.NoError(t, err)

				if ok {
					finished[i] = true
				}

				isNotFinishedAll = true
			}
		}

		if !isNotFinishedAll {
			break
		}

		time.Sleep(3 * time.Second)
	}
}
