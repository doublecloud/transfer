package mysqltoytdatetime

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	mysql_source "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/mysql"
	ytcommon "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/yt"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/runtime/local"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/worker/tasks"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

const (
	tableName           = "time_test"
	layoutDateMySQL     = "2006-01-02"
	layoutDatetimeMySQL = "2006-01-02 15:04:05.999999"
)

var (
	source        = *helpers.WithMysqlInclude(helpers.RecipeMysqlSource(), []string{tableName})
	tablePath     = ypath.Path(fmt.Sprintf("//home/cdc/test/mysql2yt/date_time/%s_%s", source.Database, tableName))
	targetCluster = os.Getenv("YT_PROXY")
)

func init() {
	source.WithDefaults()
}

func TestMain(m *testing.M) {
	ytcommon.InitExe()
	os.Exit(m.Run())
}

func makeConnConfig() *mysql.Config {
	cfg := mysql.NewConfig()
	cfg.Addr = fmt.Sprintf("%v:%v", source.Host, source.Port)
	cfg.User = source.User
	cfg.Passwd = string(source.Password)
	cfg.DBName = source.Database
	cfg.Net = "tcp"
	return cfg
}

func makeTarget() server.Destination {
	target := ytcommon.NewYtDestinationV1(ytcommon.YtDestination{
		Path:          "//home/cdc/test/mysql2yt/date_time",
		Cluster:       targetCluster,
		CellBundle:    "default",
		PrimaryMedium: "default",
	})
	target.WithDefaults()
	return target
}

func ParseDate(value string) schema.Date {
	date, _ := time.Parse(layoutDateMySQL, value)
	schemaDate, err := schema.NewDate(date)
	if err != nil {
		panic(err)
	}
	return schemaDate
}

func TestDateTime(t *testing.T) {
	targetPort, err := helpers.GetPortFromStr(targetCluster)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mysql source", Port: source.Port},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	_ = os.Setenv("YC", "1") // to not go to vanga

	ctx := context.Background()

	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	_, err = ytEnv.YT.CreateNode(ctx, ypath.Path("//home/cdc/test/mysql2yt/date_time"), yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)

	ytDestination := makeTarget()
	transfer := helpers.MakeTransfer(helpers.TransferID, &source, ytDestination, abstract.TransferTypeSnapshotAndIncrement)
	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), "test-operation", transfer, helpers.EmptyRegistry())
	err = snapshotLoader.LoadSnapshot(context.Background())
	require.NoError(t, err)

	require.NoError(t, helpers.CompareStorages(t, source, ytDestination.(ytcommon.YtDestinationModel).LegacyModel(), helpers.NewCompareStorageParams()))

	fakeClient := coordinator.NewStatefulFakeClient()
	err = mysql_source.SyncBinlogPosition(&source, transfer.ID, fakeClient)
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(fakeClient, transfer, helpers.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop() //nolint

	conn, err := mysql.NewConnector(makeConnConfig())
	require.NoError(t, err)
	db := sql.OpenDB(conn)
	_, err = db.Exec(`INSERT INTO time_test VALUES (101, '2022-12-25', '2022-12-25 14:15:16', '2022-12-25 14:15:16')`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO time_test VALUES (102, '2022-12-26', '2022-12-26 14:15:16', '2022-12-26 14:15:16')`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO time_test VALUES (103, '1970-01-01', '1970-01-01 00:00:00', '1970-01-01 00:00:00')`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO time_test VALUES (104, NULL, NULL, NULL)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO time_test VALUES (105, '1989-11-09', '1989-11-09 19:02:03.456789', '1989-11-09 19:02:03.456789')`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO time_test VALUES (106, '1970-01-01', '1970-01-01 00:00:00', '1970-01-01 00:00:00')`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO time_test VALUES (107, '2025-05-25', '2025-05-25 00:05:25.555', '2025-05-25 00:05:25.555555')`)
	require.NoError(t, err)

	require.NoError(t, helpers.WaitEqualRowsCount(t, source.Database, tableName, helpers.GetSampleableStorageByModel(t, source), helpers.GetSampleableStorageByModel(t, ytDestination.(ytcommon.YtDestinationModel).LegacyModel()), 60*time.Second))
	require.NoError(t, helpers.CompareStorages(t, source, ytDestination.(ytcommon.YtDestinationModel).LegacyModel(), helpers.NewCompareStorageParams()))
}
