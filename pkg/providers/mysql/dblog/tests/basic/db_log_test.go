package basic

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/internal/metrics"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/coordinator"
	"github.com/doublecloud/transfer/pkg/providers/mysql"
	"github.com/doublecloud/transfer/pkg/providers/mysql/dblog"
	"github.com/doublecloud/transfer/pkg/providers/mysql/mysqlrecipe"
	"github.com/doublecloud/transfer/tests/helpers"
	"github.com/stretchr/testify/require"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

var (
	Source        = mysqlrecipe.RecipeMysqlSource()
	Destination   = mysqlrecipe.RecipeMysqlTarget()
	testTableName = "__test_num_table"

	rowsAfterInserts = uint64(14)

	incrementalLimit = uint64(10)
	numberOfInserts  = 16

	sleepBetweenInserts = 100 * time.Millisecond

	minOutputItems = 15
	maxOutputItems = 30
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
}

func TestIncrementalSnapshot(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
		))
	}()
	Destination.Database = Source.Database
	sink, err := mysql.NewSinker(logger.Log, Destination, helpers.EmptyRegistry())
	require.NoError(t, err)

	arrColSchema := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "id", DataType: ytschema.TypeInt32.String(), PrimaryKey: true},
		{ColumnName: "num", DataType: ytschema.TypeInt32.String(), PrimaryKey: false},
	})
	changeItemBuilder := helpers.NewChangeItemsBuilder(Source.Database, testTableName, arrColSchema)

	require.NoError(t, sink.Push(changeItemBuilder.Inserts(t, []map[string]interface{}{{"id": 11, "num": 11}, {"id": 12, "num": 12}, {"id": 13, "num": 13}, {"id": 14, "num": 14}})))

	helpers.CheckRowsCount(t, Source, Source.Database, testTableName, rowsAfterInserts)

	mysqlStorage, err := mysql.NewStorage(Source.ToStorageParams())
	require.NoError(t, err)

	cp := coordinator.NewStatefulFakeClient()
	require.NoError(t, mysql.SyncBinlogPosition(Source, helpers.TransferID, cp))
	src, err := mysql.NewSource(
		Source,
		helpers.TransferID,
		nil,
		logger.Log,
		metrics.NewRegistry(),
		cp,
		false)
	require.NoError(t, err)

	storage, err := dblog.NewStorage(
		logger.Log,
		src,
		mysqlStorage,
		mysqlStorage.DB,
		incrementalLimit,
		helpers.TransferID,
		Source.Database,
		mysql.Represent,
	)
	require.NoError(t, err)

	sourceTables, err := storage.TableList(nil)
	require.NoError(t, err)

	var numTable *abstract.TableDescription = nil
	tables := sourceTables.ConvertToTableDescriptions()

	for _, table := range tables {
		if table.Name == testTableName {
			numTable = &table
		}
	}

	require.NotNil(t, numTable)

	var output []abstract.ChangeItem

	pusher := func(items []abstract.ChangeItem) error {
		output = append(output, items...)

		return nil
	}

	go func() {
		for i := 0; i < numberOfInserts; i++ {
			require.NoError(t, sink.Push(changeItemBuilder.Inserts(t, []map[string]interface{}{{"id": i, "num": i}})))
			time.Sleep(sleepBetweenInserts)
		}
	}()

	err = storage.LoadTable(context.Background(), *numTable, pusher)
	require.NoError(t, err)

	require.GreaterOrEqual(t, len(output), minOutputItems)
	require.LessOrEqual(t, len(output), maxOutputItems)
}
