package replication

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue/recipe"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/logbroker"
	mysqlSource "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/mysql"
	ytcommon "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/yt"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/worker/tasks"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

var Source = helpers.WithMysqlInclude(helpers.RecipeMysqlSource(), []string{"test_table"})

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
}

func TestMain(m *testing.M) {
	ytcommon.InitExe()
	os.Exit(m.Run())
}

func TestReplication(t *testing.T) {
	ctx := context.Background()

	lbEnv := recipe.New(t)

	lbSrcPort := lbEnv.ConsumerOptions().Port
	lbDstPort := lbEnv.ConsumerOptions().Port
	ytCluster := os.Getenv("YT_PROXY")
	targetPort, err := helpers.GetPortFromStr(ytCluster)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mysql source", Port: Source.Port},
			helpers.LabeledPort{Label: "LB target", Port: lbDstPort},
			helpers.LabeledPort{Label: "LB source", Port: lbSrcPort},
			helpers.LabeledPort{Label: "CH target", Port: targetPort},
		))
	}()

	// mysql -> lb

	lbDst := logbroker.LbDestination{
		Instance:        lbEnv.Endpoint,
		Topic:           lbEnv.DefaultTopic,
		Credentials:     lbEnv.ConsumerOptions().Credentials,
		WriteTimeoutSec: 60,
		Port:            lbDstPort,
		FormatSettings: server.SerializationFormat{
			Name: server.SerializationFormatNative,
		},
		TLS: logbroker.DisabledTLS,
	}
	lbDst.WithDefaults()

	transfer1 := helpers.MakeTransfer("mysql2lb", Source, &lbDst, abstract.TransferTypeIncrementOnly)

	syncBinlogPosition := func() {
		err := mysqlSource.SyncBinlogPosition(Source, "", coordinator.NewFakeClient())
		require.NoError(t, err)
	}
	syncBinlogPosition()

	// lb -> yt
	lbSrc := &logbroker.LbSource{
		Instance:    lbEnv.Endpoint,
		Topic:       lbEnv.DefaultTopic,
		Credentials: lbEnv.ConsumerOptions().Credentials,
		Consumer:    lbEnv.DefaultConsumer,
		Port:        lbSrcPort,
	}
	lbSrc.WithDefaults()

	ytTestPath := "//home/cdc/test/mysql2lb2yt_replication"

	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	_, err = ytEnv.YT.CreateNode(ctx, ypath.Path(ytTestPath), yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)

	ytDst := ytcommon.NewYtDestinationV1(
		ytcommon.YtDestination{
			Path:          ytTestPath,
			CellBundle:    "default",
			PrimaryMedium: "default",
			Cluster:       ytCluster,
			Cleanup:       server.DisabledCleanup,
		},
	)
	ytDst.WithDefaults()

	transfer2 := helpers.MakeTransfer("lb2yt", lbSrc, ytDst, abstract.TransferTypeIncrementOnly)

	// check
	conn, err := mysqlDriver.NewConnector(makeMysqlConfig(Source))
	require.NoError(t, err)
	db := sql.OpenDB(conn)
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			logger.Log.Warn("unable to close mysql db", log.Error(err))
		}
	}(db)

	ytPath := ypath.Path(fmt.Sprintf("%v/source_test_table", ytTestPath))

	readAllRowsF := func() []TestTableRow {
		return readAllRows(t, ytEnv.YT, ctx, ytPath)
	}
	checkDataWithDelay := func(expected []TestTableRow, delay time.Duration) {
		checkData(t, readAllRowsF, expected, delay)
	}
	checkDataF := func(expected []TestTableRow) {
		checkDataWithDelay(expected, 10*time.Second)
	}

	worker1 := helpers.Activate(t, transfer1)
	worker2 := helpers.Activate(t, transfer2)
	CheckInsert(t, db, checkDataF)
	CheckUpdate(t, db, checkDataF)
	CheckDelete(t, db, checkDataF)

	worker1 = CheckLbDowntime(t, db, checkDataWithDelay, worker1, transfer1, syncBinlogPosition)
	defer worker1.Close(t)

	worker2 = CheckYtDowntime(t, db, checkDataWithDelay, worker2, transfer2)
	defer worker2.Close(t)
}

func CheckInsert(t *testing.T, db *sql.DB, checkData func([]TestTableRow)) {
	_, err := db.Exec("INSERT INTO test_table (id, value) VALUES (1, 'first'), (2, 'second')")
	require.NoError(t, err)
	checkData([]TestTableRow{{ID: 1, Value: "first"}, {ID: 2, Value: "second"}})
}

func CheckUpdate(t *testing.T, db *sql.DB, checkData func([]TestTableRow)) {
	_, err := db.Exec("UPDATE test_table SET value = '2nd' WHERE id = 2")
	require.NoError(t, err)
	checkData([]TestTableRow{{ID: 1, Value: "first"}, {ID: 2, Value: "2nd"}})
}

func CheckDelete(t *testing.T, db *sql.DB, checkData func([]TestTableRow)) {
	_, err := db.Exec("DELETE FROM test_table WHERE id = 2")
	require.NoError(t, err)
	checkData([]TestTableRow{{ID: 1, Value: "first"}})
}

func CheckLbDowntime(
	t *testing.T, db *sql.DB, checkData func([]TestTableRow, time.Duration), worker *helpers.Worker,
	transfer *server.Transfer, syncBinlogPosition func(),
) *helpers.Worker {
	worker.Close(t)

	// This row will be synced with upload
	_, err := db.Exec("INSERT INTO test_table (id, value) VALUES (3, 'third')")
	require.NoError(t, err)

	// This row will be lost
	_, err = db.Exec("INSERT INTO test_table (id, value) VALUES (0, 'zero')")
	require.NoError(t, err)

	// Emulating binlog rotation
	syncBinlogPosition()
	worker = helpers.Activate(t, transfer)
	checkData([]TestTableRow{{ID: 1, Value: "first"}}, 10*time.Second)

	// Emulating upload
	worker.Close(t)
	tables := []abstract.TableDescription{{Name: "test_table", Schema: "source", Filter: "id = 3", EtaRow: 1}}
	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), "test-operation", transfer, helpers.EmptyRegistry())
	err = snapshotLoader.UploadTables(context.Background(), tables, true)
	require.NoError(t, err)
	checkData([]TestTableRow{{ID: 1, Value: "first"}, {ID: 3, Value: "third"}}, 10*time.Second)

	return helpers.Activate(t, transfer)
}

func CheckYtDowntime(
	t *testing.T, db *sql.DB, checkData func([]TestTableRow, time.Duration), worker *helpers.Worker,
	transfer *server.Transfer,
) *helpers.Worker {
	worker.Close(t)
	_, err := db.Exec("UPDATE test_table SET value = '1st' WHERE id = 1")
	require.NoError(t, err)
	checkData([]TestTableRow{{ID: 1, Value: "first"}, {ID: 3, Value: "third"}}, 5*time.Second)
	worker = helpers.Activate(t, transfer)
	checkData([]TestTableRow{{ID: 1, Value: "1st"}, {ID: 3, Value: "third"}}, 10*time.Second)
	return worker
}

type TestTableRow struct {
	ID    int    `yson:"id"`
	Value string `yson:"value"`
}

func checkData(t *testing.T, readAllRows func() []TestTableRow, expected []TestTableRow, delay time.Duration) {
	const (
		retryDelay    = 1 * time.Second
		attemptsCount = 100
	)

	time.Sleep(delay)

	for i := 0; i < attemptsCount-1; i++ {
		actual := readAllRows()
		if reflect.DeepEqual(expected, actual) {
			return
		} else {
			logger.Log.Info("values are not equal, waiting...", log.Any("expected", expected), log.Any("actual", actual))
			time.Sleep(retryDelay)
		}
	}

	require.Equal(t, expected, readAllRows())
}

func readAllRows(t *testing.T, ytClient yt.Client, ctx context.Context, ytPath ypath.Path) []TestTableRow {
	exists, err := ytClient.NodeExists(ctx, ytPath, &yt.NodeExistsOptions{})
	require.NoError(t, err)
	if !exists {
		return []TestTableRow{}
	}

	reader, err := ytClient.SelectRows(ctx, fmt.Sprintf("* FROM [%v]", ytPath), nil)
	if err != nil {
		// reading happens frequently, it is possible to catch a state when table is not dynamic yet
		logger.Log.Error("unable to select rows", log.Error(err))
		return nil
	}
	defer func(reader yt.TableReader) {
		err := reader.Close()
		if err != nil {
			logger.Log.Warn("unable to close yt reader", log.Error(err))
		}
	}(reader)

	rows := make([]TestTableRow, 0)
	for reader.Next() {
		var row TestTableRow
		err = reader.Scan(&row)
		require.NoError(t, err)
		rows = append(rows, row)
	}
	return rows
}

func makeMysqlConfig(mysqlSrc *mysqlSource.MysqlSource) *mysqlDriver.Config {
	cfg := mysqlDriver.NewConfig()
	cfg.Addr = fmt.Sprintf("%v:%v", mysqlSrc.Host, mysqlSrc.Port)
	cfg.User = mysqlSrc.User
	cfg.Passwd = string(mysqlSrc.Password)
	cfg.DBName = mysqlSrc.Database
	cfg.Net = "tcp"
	return cfg
}
