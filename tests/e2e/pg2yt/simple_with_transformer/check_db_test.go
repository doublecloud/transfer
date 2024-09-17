package replication

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/pkg/abstract"
	server "github.com/doublecloud/transfer/pkg/abstract/model"
	pgcommon "github.com/doublecloud/transfer/pkg/providers/postgres"
	ytcommon "github.com/doublecloud/transfer/pkg/providers/yt"
	"github.com/doublecloud/transfer/tests/helpers"
	yt_helpers "github.com/doublecloud/transfer/tests/helpers/yt"
	"github.com/stretchr/testify/require"
)

var (
	srcPort = helpers.GetIntFromEnv("PG_LOCAL_PORT")
	Source  = pgcommon.PgSource{
		ClusterID: os.Getenv("PG_CLUSTER_ID"),
		Hosts:     []string{"localhost"},
		User:      os.Getenv("PG_LOCAL_USER"),
		Password:  server.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:  os.Getenv("PG_LOCAL_DATABASE"),
		Port:      srcPort,
		DBTables:  []string{"public.__test"},
	}
	Target = yt_helpers.RecipeYtTarget("//home/cdc/test/pg2yt_e2e")
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

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Load", Load)
	})
}

func Load(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, Target, abstract.TransferTypeSnapshotAndIncrement)
	require.NoError(t, transfer.TransformationFromJSON(`
{
	"transformers": [
	  {
		"filterColumns": {
		  "tables": {
			"includeTables": [
				"^public.__test$"
			]
		  },
		  "columns": {
			"includeColumns": [
				"^f$",
				"^aid$",
				"^str$",
				"^id$",
				"^t$"
			]
		  }
		}
	  }
	]
}
`))
	worker := helpers.Activate(t, transfer)

	conn, err := pgcommon.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)

	_, err = conn.Exec(context.Background(), "insert into __test (str, id, da, i) values ('qqq', 111, '1999-09-16', 1)")
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(), "update __test set i=2 where str='qqq';")
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(), `insert into __test (str, id, da, i) values
                                                      ('www', 111, '1999-09-16', 1),
                                                      ('eee', 111, '1999-09-16', 1),
                                                      ('rrr', 111, '1999-09-16', 1)
    `)
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(), "delete from __test where str='rrr';")
	require.NoError(t, err)

	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "__test", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second))

	worker.Close(t)

	storage := helpers.GetSampleableStorageByModel(t, Target.LegacyModel())
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
			require.Len(t, row.TableSchema.Columns(), 5)
			require.Equal(t, []string{"aid", "str", "id", "f", "t"}, row.ColumnNames)
		}
		return nil
	}))
}
