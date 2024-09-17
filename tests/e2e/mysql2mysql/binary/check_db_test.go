package light

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/providers/mysql"
	"github.com/doublecloud/transfer/tests/helpers"
	mysql_client "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

var (
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = *helpers.RecipeMysqlSource()
	Target       = *helpers.RecipeMysqlTarget()
)

func init() {
	_ = os.Setenv("YC", "1")                                               // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mysql source", Port: Source.Port},
			helpers.LabeledPort{Label: "Mysql target", Port: Target.Port},
		))
	}()

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Existence", Existence)
		t.Run("Snapshot", Snapshot)
	})
}

func Existence(t *testing.T) {
	_, err := mysql.NewStorage(Source.ToStorageParams())
	require.NoError(t, err)
	_, err = mysql.NewStorage(Target.ToStorageParams())
	require.NoError(t, err)
}

func Snapshot(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)

	wrkr := helpers.Activate(t, transfer)

	cfg := mysql_client.NewConfig()
	cfg.Addr = fmt.Sprintf("%v:%v", Source.Host, Source.Port)
	cfg.User = Source.User
	cfg.Passwd = string(Source.Password)
	cfg.DBName = Source.Database
	cfg.Net = "tcp"

	mysqlConnector, err := mysql_client.NewConnector(cfg)
	require.NoError(t, err)
	db := sql.OpenDB(mysqlConnector)

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)

	requests := []string{
		"insert into __test values (X'11ECA452BAE6807D9FA707D7252F7EEA', 4, '{\"а\": \"3\"}');",
		"insert into __test values (X'11ECA452BB571D439FA707D7252F7EEA', 5, '{\"а\": \"3\"}');",
		"update __test set Data = '{\"updated\": \"da\"}' where Version in (1, 2, 3);",
		"delete from __test where Version = 4",
	}

	for _, request := range requests {
		rows, err := conn.QueryContext(context.Background(), request)
		require.NoError(t, err)
		require.NoError(t, rows.Close())
	}

	defer wrkr.Close(t)
	time.Sleep(20 * time.Second)
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))

	dstCfg := mysql_client.NewConfig()
	dstCfg.Addr = fmt.Sprintf("%v:%v", Target.Host, Target.Port)
	dstCfg.User = Target.User
	dstCfg.Passwd = string(Target.Password)
	dstCfg.DBName = Target.Database
	dstCfg.Net = "tcp"
	dstConnector, err := mysql_client.NewConnector(dstCfg)
	require.NoError(t, err)
	dstConn, err := sql.OpenDB(dstConnector).Conn(context.Background())
	require.NoError(t, err)
	var dstSum int
	require.NoError(t, dstConn.QueryRowContext(context.Background(), `select sum(Version) from __test;`).Scan(&dstSum))
	var srcSum int
	require.NoError(t, conn.QueryRowContext(context.Background(), `select sum(Version) from __test;`).Scan(&srcSum))
	require.Equal(t, srcSum, dstSum)
}
