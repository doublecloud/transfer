package snapshot

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/doublecloud/transfer/library/go/slices"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/clickhouse"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/clickhouse/model"
	chrecipe "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/clickhouse/recipe"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/worker/tasks"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/stretchr/testify/require"
)

var (
	databaseName = "db"
	tableName    = "test_table"
	TransferType = abstract.TransferTypeSnapshotOnly
	Source       = *chrecipe.MustSource(chrecipe.WithInitFile("dump/src.sql"), chrecipe.WithDatabase(databaseName))
	Target       = *chrecipe.MustTarget(chrecipe.WithInitFile("dump/dst.sql"), chrecipe.WithDatabase(databaseName), chrecipe.WithPrefix("DB0_"))
)

const cursorField = "Birthday"
const cursorValue = "2019-01-03"

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType)
	Source.ShardsList = []model.ClickHouseShard{
		{Name: "_", Hosts: []string{"localhost"}},
		{Name: "[", Hosts: []string{"localhost"}},
	}
	Target.Cleanup = server.DisabledCleanup
}

func TestIncrementalSnapshot(t *testing.T) {
	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "CH source", Port: Source.NativePort},
		helpers.LabeledPort{Label: "CH target", Port: Target.NativePort},
	))

	transfer := helpers.MakeTransferForIncrementalSnapshot(helpers.TransferID, &Source, &Target, TransferType, databaseName, tableName, cursorField, cursorValue, 15)
	transfer.Runtime = new(abstract.LocalRuntime)

	tables, err := tasks.ObtainAllSrcTables(transfer, helpers.EmptyRegistry())
	require.NoError(t, err)

	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewStatefulFakeClient(), "test-operation", transfer, helpers.EmptyRegistry())
	err = snapshotLoader.UploadTables(context.Background(), tables.ConvertToTableDescriptions(), true)
	require.NoError(t, err)

	require.NoError(t, helpers.WaitDestinationEqualRowsCount(databaseName, tableName, helpers.GetSampleableStorageByModel(t, Target), 60*time.Second, 7))
	// 7 and not 5, because we had to specify the same host in two shards

	conn, err := clickhouse.MakeConnection(Source.ToStorageParams(), transfer)
	require.NoError(t, err)

	addData(t, conn)

	err = snapshotLoader.UploadTables(context.Background(), tables.ConvertToTableDescriptions(), true)
	require.NoError(t, err)

	require.NoError(t, helpers.WaitDestinationEqualRowsCount(databaseName, tableName, helpers.GetSampleableStorageByModel(t, Target), 60*time.Second, 9))
	// 9 and not 8, because we had to specify the same host in two shards

	ids := readIdsFromTarget(t, helpers.GetSampleableStorageByModel(t, Target))
	require.True(t, slices.ContainsAll(ids, []uint16{1, 2, 3, 4, 5, 7}))
}

func addData(t *testing.T, conn *sql.DB) {
	query := fmt.Sprintf("INSERT INTO %s.%s (`Id`, `Name`, `Age`, `Birthday`) VALUES (7, 'Mary', 19, '2019-01-07');", databaseName, tableName)
	_, err := conn.Exec(query)
	require.NoError(t, err)
}

func readIdsFromTarget(t *testing.T, storage abstract.SampleableStorage) []uint16 {
	ids := make([]uint16, 0)

	require.NoError(t, storage.LoadTable(context.Background(), abstract.TableDescription{
		Name:   tableName,
		Schema: databaseName,
		Filter: "",
		EtaRow: 0,
		Offset: 0,
	}, func(items []abstract.ChangeItem) error {
		for _, row := range items {
			if !row.IsRowEvent() {
				continue
			}
			id := row.ColumnNameIndex("Id")
			ids = append(ids, row.ColumnValues[id].(uint16))
		}
		return nil
	}))
	return ids
}
