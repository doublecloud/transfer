package snapshot

import (
	"context"
	"os"
	"testing"

	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/mysql"
	ytcommon "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/yt"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/e2e/mysql2ch"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	yt_helpers "github.com/doublecloud/transfer/transfer_manager/go/tests/helpers/yt"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/ypath"
	yt_main "go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

var (
	Source = mysql.MysqlSource{
		Host:                os.Getenv("RECIPE_MYSQL_HOST"),
		User:                os.Getenv("RECIPE_MYSQL_USER"),
		Password:            server.SecretString(os.Getenv("RECIPE_MYSQL_PASSWORD")),
		Database:            os.Getenv("RECIPE_MYSQL_SOURCE_DATABASE"),
		Port:                helpers.GetIntFromEnv("RECIPE_MYSQL_PORT"),
		AllowDecimalAsFloat: true,
	}
	Target = yt_helpers.RecipeYtTarget("//home/cdc/test/mysql2yt_e2e_all_datatypes")
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
}

func TestMain(m *testing.M) {
	ytcommon.InitExe()
	os.Exit(m.Run())
}

func TestSnapshot(t *testing.T) {
	targetPort, err := helpers.GetPortFromStr(Target.Cluster())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "MySQL source", Port: Source.Port},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	ctx := context.Background()

	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()
	_, err = ytEnv.YT.CreateNode(ctx, ypath.Path("//home/cdc/test/mysql2yt_e2e_all_datatypes"), yt_main.NodeMap, &yt_main.CreateNodeOptions{Recursive: true})
	defer func() {
		err := ytEnv.YT.RemoveNode(ctx, ypath.Path("//home/cdc/test/mysql2yt_e2e_all_datatypes"), &yt_main.RemoveNodeOptions{Recursive: true})
		require.NoError(t, err)
	}()
	require.NoError(t, err)

	targetForCompare, ok := Target.(*ytcommon.YtDestinationWrapper)
	require.True(t, ok)

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, Target, abstract.TransferTypeSnapshotOnly)

	_ = helpers.Activate(t, transfer)
	require.NoError(t, helpers.CompareStorages(t, &Source, targetForCompare, helpers.NewCompareStorageParams().WithPriorityComparators(mysql2ch.MySQLBytesToStringOptionalComparator)))
}
