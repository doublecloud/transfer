package replication

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/logbroker"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/postgres"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/yt"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/runtime/local"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers/lbenv"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/ypath"
	ytMain "go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

var (
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
	Target = yt.NewYtDestinationV1(yt.YtDestination{
		Path:          "//home/cdc/test/pg2lb2yt_e2e_replication",
		Cluster:       os.Getenv("YT_PROXY"),
		CellBundle:    "default",
		PrimaryMedium: "default",
	})
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
	Target.WithDefaults()
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

	defer func() {
		err := ytEnv.YT.RemoveNode(context.Background(), ypath.Path("//home/cdc/test/pg2lb2yt_e2e_replication"), &ytMain.RemoveNodeOptions{Recursive: true})
		require.NoError(t, err)
	}()

	t.Run("Load", Load)
}

func getTableName(t abstract.TableDescription) string {
	if t.Schema == "" || t.Schema == "public" {
		return t.Name
	}

	return t.Schema + "_" + t.Name
}

func closeReader(reader ytMain.TableReader) {
	err := reader.Close()
	if err != nil {
		logger.Log.Warn("Could not close table reader")
	}
}

func checkRowCount(yt ytMain.Client, tablePath ypath.Path, rowsNumber int) (bool, error) {
	reader, err := yt.SelectRows(context.Background(), fmt.Sprintf("SUM(1) AS row_count FROM [%s] GROUP BY 1", tablePath), &ytMain.SelectRowsOptions{})
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
	if result["row_count"] == rowsNumber {
		return true, nil
	}

	return false, nil
}

func waitForRows(t *testing.T, tablePaths []ypath.Path, rowsNumber int) {
	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	finished := make([]bool, len(tablePaths))

	for {
		isNotFinishedAll := false

		for i, tablePath := range tablePaths {
			if !finished[i] {
				ok, err := checkRowCount(ytEnv.YT, tablePath, rowsNumber)
				// first tries may return errors, i.e. not existing node
				if err != nil {
					logger.Log.Infof("checkRowCount error %v", err)
				}

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

func closeWorker(worker *local.LocalWorker) {
	err := worker.Stop()
	if err != nil {
		logger.Log.Infof("unable to close worker %v", worker.Runtime())
	}
}

func Load(t *testing.T) {
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

	transfer1 := server.Transfer{
		ID:  "test_id_pg2lb",
		Src: &Source,
		Dst: &dst,
	}

	err = postgres.CreateReplicationSlot(&Source)
	require.NoError(t, err)

	w1 := local.NewLocalWorker(coordinator.NewFakeClient(), &transfer1, helpers.EmptyRegistry(), logger.Log)
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

	transfer2 := server.Transfer{
		ID:  "test_id_lb2yt",
		Src: src,
		Dst: Target,
	}

	w2 := local.NewLocalWorker(coordinator.NewFakeClient(), &transfer2, helpers.EmptyRegistry(), logger.Log)
	w2.Start()
	defer closeWorker(w2)

	require.NoError(t, err)

	//------------------------------------------------------------------------------

	time.Sleep(time.Second * 10)

	//------------------------------------------------------------------------------

	insertQuery := "INSERT INTO public.__test (a_id, a_name) VALUES (7, 'DT is a team of an amazing people');"
	_, err = srcConn.Exec(context.Background(), insertQuery)
	require.NoError(t, err)

	tablePaths := []ypath.Path{
		ypath.Path(Target.Path()).Child(getTableName(abstract.TableDescription{Name: "__test"})),
	}

	waitForRows(t, tablePaths, 1)
}
