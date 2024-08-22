package light

import (
	"os"
	"testing"

	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/middlewares"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/sink"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/worker/tasks"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/worker/tasks/cleanup"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/stretchr/testify/require"
)

var (
	Source              = *helpers.RecipeMysqlSource()
	SourceWithBlackList = *helpers.WithMysqlInclude(helpers.RecipeMysqlSource(), []string{"items_.*"})
	Target              = *helpers.RecipeMysqlTarget()
)

func init() {
	_ = os.Setenv("YC", "1")                                                                            // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mysql source", Port: Source.Port},
			helpers.LabeledPort{Label: "Mysql target", Port: Target.Port},
		))
	}()

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Drop by filter", TruncateAll)
		t.Run("Drop by filter", DropFilter)
		t.Run("Drop all tables", DropAll)
	})
}

func DropAll(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement)

	tables, err := tasks.ObtainAllSrcTables(transfer, helpers.EmptyRegistry())
	require.NoError(t, err)
	logger.Log.Infof("got tables: %v", tables)

	sink, err := sink.MakeAsyncSink(transfer, logger.Log, helpers.EmptyRegistry(), coordinator.NewFakeClient(), middlewares.MakeConfig(middlewares.WithNoData))
	require.NoError(t, err)

	err = cleanup.CleanupTables(sink, tables, server.Drop)
	require.NoError(t, err)
}

func DropFilter(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &SourceWithBlackList, &Target, abstract.TransferTypeSnapshotAndIncrement)

	tables, err := tasks.ObtainAllSrcTables(transfer, helpers.EmptyRegistry())
	require.NoError(t, err)
	logger.Log.Infof("got tables: %v", tables)

	sink, err := sink.MakeAsyncSink(transfer, logger.Log, helpers.EmptyRegistry(), coordinator.NewFakeClient(), middlewares.MakeConfig(middlewares.WithNoData))
	require.NoError(t, err)

	err = cleanup.CleanupTables(sink, tables, server.Drop)
	require.NoError(t, err)
}

func TruncateAll(t *testing.T) {
	dstCopy := Target
	dstCopy.Cleanup = server.Truncate
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &dstCopy, abstract.TransferTypeSnapshotAndIncrement)

	tables, err := tasks.ObtainAllSrcTables(transfer, helpers.EmptyRegistry())
	require.NoError(t, err)
	logger.Log.Infof("got tables: %v", tables)

	sink, err := sink.MakeAsyncSink(transfer, logger.Log, helpers.EmptyRegistry(), coordinator.NewFakeClient(), middlewares.MakeConfig(middlewares.WithNoData))
	require.NoError(t, err)

	err = cleanup.CleanupTables(sink, tables, server.Truncate)
	require.NoError(t, err)
}
