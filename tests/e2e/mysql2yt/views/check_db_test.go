package snapshot

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/coordinator"
	"github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/providers/mysql"
	"github.com/doublecloud/transfer/pkg/providers/yt"
	"github.com/doublecloud/transfer/pkg/worker/tasks"
	"github.com/doublecloud/transfer/tests/helpers"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/ypath"
	yt_main "go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

var (
	source = mysql.MysqlSource{
		Host:     os.Getenv("RECIPE_MYSQL_HOST"),
		User:     os.Getenv("RECIPE_MYSQL_USER"),
		Password: model.SecretString(os.Getenv("RECIPE_MYSQL_PASSWORD")),
		Database: os.Getenv("RECIPE_MYSQL_SOURCE_DATABASE"),
		Port:     helpers.GetIntFromEnv("RECIPE_MYSQL_PORT"),
	}
	target = yt.NewYtDestinationV1(yt.YtDestination{
		Path:    "//home/cdc/test/mysql2yt_e2e_snapshot",
		Cluster: os.Getenv("YT_PROXY"),
		Static:  true,
	})
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	source.WithDefaults()
	target.WithDefaults()
}

func TestGroup(t *testing.T) {
	targetPort, err := helpers.GetPortFromStr(target.Cluster())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mysql source", Port: source.Port},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	ctx := context.Background()

	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	_, err = ytEnv.YT.CreateNode(ctx, ypath.Path("//home/cdc/test/mysql2yt_e2e_snapshot"), yt_main.NodeMap, &yt_main.CreateNodeOptions{Recursive: true})
	defer func() {
		err := ytEnv.YT.RemoveNode(ctx, ypath.Path("//home/cdc/test/mysql2yt_e2e_snapshot"), &yt_main.RemoveNodeOptions{Recursive: true})
		require.NoError(t, err)
	}()
	require.NoError(t, err)

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Existence", Existence)
		t.Run("Snapshot", Snapshot)
	})
}

func Existence(t *testing.T) {
	helpers.GetSampleableStorageByModel(t, source)
	helpers.GetSampleableStorageByModel(t, target.LegacyModel().(*yt.YtDestination))
}

func Snapshot(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &source, target, abstract.TransferTypeSnapshotOnly)
	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewStatefulFakeClient(), "test-operation", transfer, helpers.EmptyRegistry())
	require.NoError(t, snapshotLoader.LoadSnapshot(context.Background()))
	require.NoError(t, helpers.WaitEqualRowsCount(t, source.Database, "__test_view", helpers.GetSampleableStorageByModel(t, source), helpers.GetSampleableStorageByModel(t, target.LegacyModel()), 10*time.Second))
	require.NoError(t, helpers.WaitEqualRowsCount(t, source.Database, "__test", helpers.GetSampleableStorageByModel(t, source), helpers.GetSampleableStorageByModel(t, target.LegacyModel()), 10*time.Second))
}
