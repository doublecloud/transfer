package replication

import (
	"os"
	"testing"
	"time"

	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/clickhouse/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/logbroker"
	mysql2 "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/mysql"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/runtime/local"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers/lbenv"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

var (
	databaseName = "source"
	TransferType = abstract.TransferTypeIncrementOnly
	Source       = mysql2.MysqlSource{
		Host:     os.Getenv("RECIPE_MYSQL_HOST"),
		User:     os.Getenv("RECIPE_MYSQL_USER"),
		Password: server.SecretString(os.Getenv("RECIPE_MYSQL_PASSWORD")),
		Database: os.Getenv("RECIPE_MYSQL_SOURCE_DATABASE"),
		Port:     helpers.GetIntFromEnv("RECIPE_MYSQL_PORT"),
		ServerID: 1, // what is it?
	}
	Target = model.ChDestination{
		ShardsList: []model.ClickHouseShard{
			{
				Name: "_",
				Hosts: []string{
					"localhost",
				},
			},
		},
		User:                "default",
		Password:            "",
		Database:            databaseName,
		HTTPPort:            helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT"),
		NativePort:          helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT"),
		ProtocolUnspecified: true,
	}
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
	Target.WithDefaults()
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mysql source", Port: Source.Port},
			helpers.LabeledPort{Label: "CH target", Port: Target.NativePort},
		))
	}()

	t.Run("Load", Load)
}

func closeWorker(worker *local.LocalWorker) {
	err := worker.Stop()
	if err != nil {
		logger.Log.Infof("unable to close worker %v", worker.Runtime())
	}
}

func Load(t *testing.T) {
	lbEnv, stop := lbenv.NewLbEnv(t)

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "LB target", Port: lbEnv.ConsumerOptions().Port},
		))
	}()
	defer stop()

	//------------------------------------------------------------------------------
	// pg -> lb

	dst := logbroker.LbDestination{
		Instance:        lbEnv.Endpoint,
		Topic:           lbEnv.DefaultTopic,
		Credentials:     lbEnv.ConsumerOptions().Credentials,
		WriteTimeoutSec: 60,
		Port:            lbEnv.ConsumerOptions().Port,
		FormatSettings: server.SerializationFormat{
			Name: server.SerializationFormatNative,
		},
		TLS: logbroker.DisabledTLS,
	}
	dst.WithDefaults()

	transfer1 := helpers.MakeTransfer(helpers.TransferID, &Source, &dst, TransferType)

	fakeClient := coordinator.NewStatefulFakeClient()
	err := mysql2.SyncBinlogPosition(&Source, transfer1.ID, fakeClient)
	require.NoError(t, err)

	w1 := local.NewLocalWorker(fakeClient, transfer1, helpers.EmptyRegistry(), logger.Log)
	w1.Start()
	defer closeWorker(w1)

	require.NoError(t, err)

	//------------------------------------------------------------------------------
	// lb -> yt

	src := &logbroker.LbSource{
		Instance:    lbEnv.Endpoint,
		Topic:       lbEnv.DefaultTopic,
		Credentials: lbEnv.ConsumerOptions().Credentials,
		Consumer:    lbEnv.DefaultConsumer,
		Port:        lbEnv.ConsumerOptions().Port,
	}
	src.WithDefaults()

	transfer2 := helpers.MakeTransfer(helpers.TransferID, src, &Target, TransferType)

	w2 := local.NewLocalWorker(coordinator.NewFakeClient(), transfer2, helpers.EmptyRegistry(), logger.Log)
	w2.Start()
	defer closeWorker(w2)

	require.NoError(t, err)

	//------------------------------------------------------------------------------

	queries := []string{
		"INSERT INTO source.__test (a_id, a_name) VALUES (0, 'str_val_0'), (1, 'str_val_1'), (2, 'str_val_2');",
		"UPDATE source.__test SET a_name='qwe' WHERE a_id=2",
		"DELETE FROM source.__test WHERE a_id=1",
	}

	connectionParams, err := mysql2.NewConnectionParams(Source.ToStorageParams())
	require.NoError(t, err)
	configAction := func(config *mysql.Config) error {
		config.MultiStatements = true
		config.MaxAllowedPacket = 0
		return nil
	}
	conn, _ := mysql2.Connect(connectionParams, configAction)

	for _, query := range queries {
		_, err = conn.Exec(query)
		require.NoError(t, err)
	}

	require.NoError(t, helpers.WaitDestinationEqualRowsCount(Source.Database, "__test", helpers.GetSampleableStorageByModel(t, Target), 60*time.Second, 2))
}
