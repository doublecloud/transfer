package light

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/mysql"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/postgres"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/stretchr/testify/require"
)

var (
	TransferType = abstract.TransferTypeSnapshotOnly

	Source = *helpers.RecipeMysqlSource()

	dstPort, _ = strconv.Atoi(os.Getenv("PG_LOCAL_PORT"))
	Target     = postgres.PgDestination{
		Hosts:     []string{"localhost"},
		ClusterID: os.Getenv("TARGET_CLUSTER_ID"),
		User:      os.Getenv("PG_LOCAL_USER"),
		Password:  server.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:  os.Getenv("PG_LOCAL_DATABASE"),
		Port:      dstPort,
		Cleanup:   server.Drop,
	}
)

func init() {
	_ = os.Setenv("YC", "1")                                               // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mysql source", Port: Source.Port},
			helpers.LabeledPort{Label: "Pg target", Port: Target.Port},
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
	_, err = postgres.NewStorage(Target.ToStorageParams())
	require.NoError(t, err)
}

func Snapshot(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)
	_ = helpers.Activate(t, transfer)

	require.NoError(t, helpers.WaitDestinationEqualRowsCount(
		"source",
		"__test",
		helpers.GetSampleableStorageByModel(t, Source),
		60*time.Second,
		3,
	),
	)

	require.NoError(t, helpers.WaitDestinationEqualRowsCount(
		"source",
		"__test2",
		helpers.GetSampleableStorageByModel(t, Source),
		60*time.Second,
		2,
	),
	)
}
