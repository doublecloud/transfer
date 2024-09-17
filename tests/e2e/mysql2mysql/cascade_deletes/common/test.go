package cascadedeletescommon

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/coordinator"
	"github.com/doublecloud/transfer/pkg/providers/mysql"
	"github.com/doublecloud/transfer/pkg/runtime/local"
	"github.com/doublecloud/transfer/pkg/worker/tasks"
	"github.com/doublecloud/transfer/tests/helpers"
	mysql_client "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

var (
	TransferType = abstract.TransferTypeIncrementOnly
	Source       = helpers.RecipeMysqlSource()
	Target       = helpers.RecipeMysqlTarget()
)

func init() {
	helpers.InitSrcDst(helpers.TransferID, Source, Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func Existence(t *testing.T) {
	_, err := mysql.NewStorage(Source.ToStorageParams())
	require.NoError(t, err)
	_, err = mysql.NewStorage(Target.ToStorageParams())
	require.NoError(t, err)
}

func Snapshot(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, Source, Target, abstract.TransferTypeSnapshotOnly)
	require.NoError(t, tasks.ActivateDelivery(context.TODO(), nil, coordinator.NewFakeClient(), *transfer, helpers.EmptyRegistry()))
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}

func Load(t *testing.T) {
	sourceAsDestination := mysql.MysqlDestination{
		Host:     Source.Host,
		User:     Source.User,
		Password: Source.Password,
		Database: Source.Database,
		Port:     Source.Port,
	}
	sourceAsDestination.WithDefaults()
	_, err := mysql.NewSinker(logger.Log, &sourceAsDestination, helpers.EmptyRegistry())
	require.NoError(t, err)

	transfer := helpers.MakeTransfer(helpers.TransferID, Source, Target, TransferType)

	fakeClient := coordinator.NewStatefulFakeClient()
	err = mysql.SyncBinlogPosition(Source, transfer.ID, fakeClient)
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(fakeClient, transfer, helpers.EmptyRegistry(), logger.Log)
	localWorker.Start()
	// defer localWorker.Stop() // Uncommenting makes test crash

	tables, err := tasks.ObtainAllSrcTables(transfer, helpers.EmptyRegistry())
	require.NoError(t, err)
	logger.Log.Infof("Tables on source: %v", tables)

	sourceCfg := mysql_client.NewConfig()
	sourceCfg.Addr = fmt.Sprintf("%v:%v", Source.Host, Source.Port)
	sourceCfg.User = Source.User
	sourceCfg.Passwd = string(Source.Password)
	sourceCfg.DBName = Source.Database
	sourceCfg.Net = "tcp"

	sourceMysqlConnector, err := mysql_client.NewConnector(sourceCfg)
	require.NoError(t, err)
	sourceDB := sql.OpenDB(sourceMysqlConnector)

	sourceConn, err := sourceDB.Conn(context.Background())
	require.NoError(t, err)

	tx, err := sourceConn.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
	require.NoError(t, err)

	cascadeDeleteRequest := "DELETE FROM `__test_A` WHERE `a_id`=2;"

	_, err = tx.Exec(`CREATE TABLE test_create (
				 id integer NOT NULL AUTO_INCREMENT PRIMARY KEY
            ) engine=innodb default charset=utf8`)
	require.NoError(t, err)

	_, err = tx.Query(cascadeDeleteRequest)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)
	err = sourceConn.Close()
	require.NoError(t, err)

	require.NoError(t, helpers.WaitEqualRowsCountDifferentSchemas(t,
		Source.Database, Target.Database, "__test_A",
		helpers.GetSampleableStorageByModel(t, Source),
		helpers.GetSampleableStorageByModel(t, Target),
		60*time.Second))
	require.NoError(t, helpers.WaitEqualRowsCountDifferentSchemas(t,
		Source.Database, Target.Database, "__test_B",
		helpers.GetSampleableStorageByModel(t, Source),
		helpers.GetSampleableStorageByModel(t, Target),
		60*time.Second))
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}
