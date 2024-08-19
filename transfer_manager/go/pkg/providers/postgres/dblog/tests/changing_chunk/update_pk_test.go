package changingchunk

import (
	"context"
	"os"
	"testing"

	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/metrics"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	pgsink "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/postgres"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/postgres/dblog"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/stats"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/stretchr/testify/require"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

var (
	numberOfUpdates = 10
	slotIDSuffix    = "updatepk"
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
}

func TestUpdateKey(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
		))
	}()

	Source.SlotID += slotIDSuffix

	sinkParams := Source.ToSinkParams()
	sink, err := pgsink.NewSink(logger.Log, helpers.TransferID, sinkParams, helpers.EmptyRegistry())
	require.NoError(t, err)

	arrColSchema := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "id", DataType: ytschema.TypeInt32.String(), PrimaryKey: true},
		{ColumnName: "num", DataType: ytschema.TypeInt32.String(), PrimaryKey: false},
	})
	changeItemBuilder := helpers.NewChangeItemsBuilder("public", testTableName, arrColSchema)

	cnt := 0

	opt := func() {
		for i := 1; cnt == 0 && i <= numberOfUpdates; i++ {
			require.NoError(t, sink.Push(changeItemBuilder.Updates(t, []map[string]interface{}{{"id": i - 10, "num": i}}, []map[string]interface{}{{"id": i, "num": i}})))
		}
		cnt++
	}

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

	storage, err := dblog.NewStorage(src, pgStorage, pgStorage.Conn, incrementalLimit, Source.SlotID, pgsink.Represent, opt)
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

	err = storage.LoadTable(context.Background(), *numTable, pusher)
	require.NoError(t, err)

	require.Equal(t, numberOfUpdates, len(output))
}
