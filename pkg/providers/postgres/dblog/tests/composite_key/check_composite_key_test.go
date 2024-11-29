package compositekey

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/internal/metrics"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/coordinator"
	pgsink "github.com/doublecloud/transfer/pkg/providers/postgres"
	"github.com/doublecloud/transfer/pkg/providers/postgres/dblog"
	"github.com/doublecloud/transfer/pkg/providers/postgres/pgrecipe"
	"github.com/doublecloud/transfer/pkg/stats"
	"github.com/doublecloud/transfer/tests/helpers"
	"github.com/stretchr/testify/require"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

var (
	Source        = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump"), pgrecipe.WithPrefix(""))
	testTableName = "text_int_pk"

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
	sinkParams := Source.ToSinkParams()
	sink, err := pgsink.NewSink(logger.Log, helpers.TransferID, sinkParams, helpers.EmptyRegistry())
	require.NoError(t, err)

	arrColSchema := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "text_pk", DataType: ytschema.TypeString.String(), PrimaryKey: true},
		{ColumnName: "int_pk", DataType: ytschema.TypeInt32.String(), PrimaryKey: true},
	})
	changeItemBuilder := helpers.NewChangeItemsBuilder("public", testTableName, arrColSchema)

	require.NoError(t, sink.Push(changeItemBuilder.Inserts(t, []map[string]interface{}{{"text_pk": "11", "int_pk": 11}, {"text_pk": 12, "int_pk": 12}, {"text_pk": 13, "int_pk": 13}, {"text_pk": 14, "int_pk": 14}})))

	helpers.CheckRowsCount(t, Source, "public", testTableName, rowsAfterInserts)

	pgStorage, err := pgsink.NewStorage(Source.ToStorageParams(nil))
	require.NoError(t, err)

	err = pgsink.CreateReplicationSlot(&Source)
	require.NoError(t, err)

	src, err := pgsink.NewSourceWrapper(
		&Source,
		Source.SlotID,
		nil,
		logger.Log,
		stats.NewSourceStats(metrics.NewRegistry()),
		coordinator.NewFakeClient())
	require.NoError(t, err)

	storage, err := dblog.NewStorage(logger.Log, src, pgStorage, pgStorage.Conn, incrementalLimit, Source.SlotID, "public", pgsink.Represent)
	require.NoError(t, err)

	sourceTables, err := storage.TableList(nil)
	require.NoError(t, err)

	var compositeKeyTable *abstract.TableDescription = nil
	tables := sourceTables.ConvertToTableDescriptions()

	for _, table := range tables {
		if table.Name == testTableName {
			compositeKeyTable = &table
		}
	}

	require.NotNil(t, compositeKeyTable)

	var output []abstract.ChangeItem

	pusher := func(items []abstract.ChangeItem) error {
		output = append(output, items...)
		return nil
	}

	go func() {
		for i := 0; i < numberOfInserts; i++ {
			require.NoError(t, sink.Push(changeItemBuilder.Inserts(t, []map[string]interface{}{{"text_pk": fmt.Sprintf("%d", i), "int_pk": i}})))
			time.Sleep(sleepBetweenInserts)
		}
	}()

	err = storage.LoadTable(context.Background(), *compositeKeyTable, pusher)
	require.NoError(t, err)

	require.LessOrEqual(t, minOutputItems, len(output))
	require.LessOrEqual(t, len(output), maxOutputItems)
}
