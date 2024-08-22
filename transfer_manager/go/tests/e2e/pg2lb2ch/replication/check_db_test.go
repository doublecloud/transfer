package replication

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/clickhouse/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/logbroker"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/postgres"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/runtime/local"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers/lbenv"
	"github.com/stretchr/testify/require"
)

var (
	databaseName = "public"

	Source = postgres.PgSource{
		ClusterID: os.Getenv("PG_CLUSTER_ID"),
		Hosts:     []string{"localhost"},
		User:      os.Getenv("PG_LOCAL_USER"),
		Password:  server.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:  os.Getenv("PG_LOCAL_DATABASE"),
		Port:      helpers.GetIntFromEnv("PG_LOCAL_PORT"),
		DBTables:  []string{"public.__test"},
		SlotID:    "test_slot_id",
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
		User:       "default",
		Password:   "",
		Database:   databaseName,
		HTTPPort:   helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT"),
		NativePort: helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT"),
	}
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
	Target.WithDefaults()
}

func closeWorker(worker *local.LocalWorker) {
	err := worker.Stop()
	if err != nil {
		logger.Log.Infof("unable to close worker %v", worker.Runtime())
	}
}

func TestReplication(t *testing.T) {
	srcConnConfig, err := postgres.MakeConnConfigFromSrc(logger.Log, &Source)
	require.NoError(t, err)
	srcConnConfig.PreferSimpleProtocol = true
	srcConn, err := postgres.NewPgConnPool(srcConnConfig, nil)
	require.NoError(t, err)

	createQuery := "create table IF NOT EXISTS __test (a_id integer not null primary key, a_name varchar(255) not null);"
	_, err = srcConn.Exec(context.Background(), createQuery)
	require.NoError(t, err)

	//------------------------------------------------------------------------------

	lbEnv, stop := lbenv.NewLbEnv(t)

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "LB source/target", Port: lbEnv.ConsumerOptions().Port},
			helpers.LabeledPort{Label: "CH target", Port: Target.NativePort},
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

	transfer1 := helpers.MakeTransfer(helpers.TransferID, &Source, &dst, abstract.TransferTypeSnapshotAndIncrement)

	err = postgres.CreateReplicationSlot(&Source)
	require.NoError(t, err)

	w1 := local.NewLocalWorker(coordinator.NewFakeClient(), transfer1, helpers.EmptyRegistry(), logger.Log)
	w1.Start()
	defer closeWorker(w1)

	require.NoError(t, err)

	//------------------------------------------------------------------------------
	// lb -> ch

	src := &logbroker.LbSource{
		Instance:    lbEnv.Endpoint,
		Topic:       lbEnv.DefaultTopic,
		Credentials: lbEnv.ConsumerOptions().Credentials,
		Consumer:    lbEnv.DefaultConsumer,
		Port:        lbEnv.ConsumerOptions().Port,
	}
	src.WithDefaults()

	transfer2 := helpers.MakeTransfer(helpers.TransferID, src, &Target, abstract.TransferTypeIncrementOnly)

	w2 := local.NewLocalWorker(coordinator.NewFakeClient(), transfer2, helpers.EmptyRegistry(), logger.Log)
	w2.Start()
	defer closeWorker(w2)

	require.NoError(t, err)

	//------------------------------------------------------------------------------

	queries := []string{
		"INSERT INTO public.__test (a_id, a_name) VALUES (0, 'str_val_0'), (1, 'str_val_1'), (2, 'str_val_2');",
		"UPDATE public.__test SET a_name='qwe' WHERE a_id=2",
		"DELETE FROM public.__test WHERE a_id=1",
	}

	for _, query := range queries {
		_, err = srcConn.Exec(context.Background(), query)
		require.NoError(t, err)
	}

	require.NoError(t, helpers.WaitDestinationEqualRowsCount("public", "__test", helpers.GetSampleableStorageByModel(t, Target), 60*time.Second, 2))
}
