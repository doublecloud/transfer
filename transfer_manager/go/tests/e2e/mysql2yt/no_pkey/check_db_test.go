package nopkey

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/mysql"
	yt2 "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/yt"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/runtime/local"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/worker/tasks"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

var (
	ctx                  = context.Background()
	expectedTableContent = makeExpectedTableContent()
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

func makeExpectedTableContent() (result []string) {
	for i := 1; i <= 20; i++ {
		result = append(result, fmt.Sprintf("%d", i))
	}
	return
}

type fixture struct {
	t            *testing.T
	transfer     server.Transfer
	ytEnv        *yttest.Env
	destroyYtEnv func()
}

type ytRow struct {
	Value string `yson:"value"`
}

func (f *fixture) teardown() {
	forceRemove := &yt.RemoveNodeOptions{Force: true}
	err := f.ytEnv.YT.RemoveNode(ctx, ypath.Path("//home/cdc/mysql2yt_e2e_no_pkey/source_test"), forceRemove)
	require.NoError(f.t, err)
	f.destroyYtEnv()
}

func (f *fixture) readAll() (result []string) {
	reader, err := f.ytEnv.YT.ReadTable(ctx, ypath.Path("//home/cdc/mysql2yt_e2e_no_pkey/source_test"), &yt.ReadTableOptions{})
	require.NoError(f.t, err)
	defer reader.Close()

	for reader.Next() {
		var row ytRow
		require.NoError(f.t, reader.Scan(&row))
		result = append(result, row.Value)
	}
	require.NoError(f.t, reader.Err())
	return
}

func makeTarget() server.Destination {
	target := yt2.NewYtDestinationV1(yt2.YtDestination{
		Path:          "//home/cdc/mysql2yt_e2e_no_pkey",
		Cluster:       os.Getenv("YT_PROXY"),
		CellBundle:    "default",
		PrimaryMedium: "default",
	})
	target.WithDefaults()
	return target
}

func setup(t *testing.T) *fixture {
	ytEnv, destroyYtEnv := yttest.NewEnv(t)

	return &fixture{
		t: t,
		transfer: server.Transfer{
			ID:  "dttwhatever",
			Src: helpers.RecipeMysqlSource(),
			Dst: makeTarget(),
		},
		ytEnv:        ytEnv,
		destroyYtEnv: destroyYtEnv,
	}
}

func srcAndDstPorts(fxt *fixture) (int, int, error) {
	sourcePort := fxt.transfer.Src.(*mysql.MysqlSource).Port
	ytCluster := fxt.transfer.Dst.(yt2.YtDestinationModel).Cluster()
	targetPort, err := helpers.GetPortFromStr(ytCluster)
	if err != nil {
		return 1, 1, err
	}
	return sourcePort, targetPort, err
}

func TestSnapshotOnlyWorksWithStaticTables(t *testing.T) {
	fixture := setup(t)

	sourcePort, targetPort, err := srcAndDstPorts(fixture)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mysql source", Port: sourcePort},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	defer fixture.teardown()
	fixture.transfer.Dst.(yt2.YtDestinationModel).SetStaticTable()
	fixture.transfer.Type = abstract.TransferTypeSnapshotOnly

	err = tasks.ActivateDelivery(context.TODO(), nil, coordinator.NewStatefulFakeClient(), fixture.transfer, helpers.EmptyRegistry())
	require.NoError(t, err)
	require.EqualValues(t, expectedTableContent, fixture.readAll())
}

func TestSnapshotOnlyFailsWithSortedTables(t *testing.T) {
	fixture := setup(t)

	sourcePort, targetPort, err := srcAndDstPorts(fixture)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mysql source", Port: sourcePort},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	defer fixture.teardown()
	fixture.transfer.Type = abstract.TransferTypeSnapshotOnly

	err = tasks.ActivateDelivery(context.TODO(), nil, coordinator.NewStatefulFakeClient(), fixture.transfer, helpers.EmptyRegistry())
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "no key columns found")

	wrk := local.NewLocalWorker(coordinator.NewStatefulFakeClient(), &fixture.transfer, helpers.EmptyRegistry(), logger.Log)
	err = wrk.Run()
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "no key columns found")
}

func TestIncrementFails(t *testing.T) {
	test := func(transferType abstract.TransferType) {
		fixture := setup(t)

		sourcePort, targetPort, err := srcAndDstPorts(fixture)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, helpers.CheckConnections(
				helpers.LabeledPort{Label: "Mysql source", Port: sourcePort},
				helpers.LabeledPort{Label: "YT target", Port: targetPort},
			))
		}()

		defer fixture.teardown()
		fixture.transfer.Type = transferType

		err = tasks.ActivateDelivery(context.TODO(), nil, coordinator.NewStatefulFakeClient(), fixture.transfer, helpers.EmptyRegistry())
		require.Error(t, err)
		require.Contains(t, strings.ToLower(err.Error()), "no key columns found")

		wrk := local.NewLocalWorker(coordinator.NewStatefulFakeClient(), &fixture.transfer, helpers.EmptyRegistry(), logger.Log)
		err = wrk.Run()
		require.Error(t, err)
		require.Contains(t, strings.ToLower(err.Error()), "no key columns found")
	}

	for _, transferType := range []abstract.TransferType{abstract.TransferTypeIncrementOnly, abstract.TransferTypeSnapshotAndIncrement} {
		test(transferType)
	}
}
