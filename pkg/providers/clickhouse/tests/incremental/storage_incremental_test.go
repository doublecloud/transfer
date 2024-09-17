package incremental

import (
	"context"
	"testing"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/pkg/abstract"
	server "github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/providers/clickhouse"
	"github.com/doublecloud/transfer/pkg/providers/clickhouse/conn"
	chrecipe "github.com/doublecloud/transfer/pkg/providers/clickhouse/recipe"
	"github.com/stretchr/testify/require"
)

func TestIncrementalShardedStorage(t *testing.T) {
	var (
		incrementalDB = chrecipe.MustSource(
			chrecipe.WithDatabase("incrementalns"),
			chrecipe.WithInitFile("incremental.sql"),
		)
	)
	shard1, err := clickhouse.NewStorage(incrementalDB.ToStorageParams(), new(server.Transfer))
	require.NoError(t, err)
	ctx := context.Background()

	incrementalStorage, isIncrementalStorage := shard1.(*clickhouse.Storage)
	require.True(t, isIncrementalStorage, "should be incremental storage")

	t.Run("incremental timestamp", func(t *testing.T) {
		conn, err := conn.ConnectNative("localhost", incrementalDB.ToSinkParams())
		require.NoError(t, err)
		defer conn.Close()

		_, err = conn.Exec(`create table test_table (id int, created_at DateTime(9)) ENGINE = MergeTree() order by id;`)
		require.NoError(t, err)

		res, err := incrementalStorage.GetIncrementalState(ctx, []abstract.IncrementalTable{{
			Name:        "test_table",
			Namespace:   "incrementalns",
			CursorField: "created_at",
		}})
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Len(t, res, 1)

		tableElementsCount := 0
		for _, incrementSize := range []int{5, 10, 15} {
			from := tableElementsCount
			_, err = conn.Exec(`
insert into test_table
select number as id, parseDateTime64BestEffort('2020-01-01', 9) + number as created_at FROM numbers($1, $2);`, from, incrementSize)
			require.NoError(t, err)

			var incrementRes []abstract.ChangeItem
			for _, tdesc := range res {
				require.NoError(t, shard1.LoadTable(context.Background(), tdesc, func(input []abstract.ChangeItem) error {
					for _, row := range input {
						if row.IsRowEvent() {
							incrementRes = append(incrementRes, row)
						}
					}
					return nil
				}))
			}
			abstract.Dump(incrementRes)
			logger.Log.Infof("count: %v", len(incrementRes))
			require.Equal(t, incrementSize, len(incrementRes))
			tableElementsCount += incrementSize

			res, err = incrementalStorage.GetIncrementalState(ctx, []abstract.IncrementalTable{{
				Name:        "test_table",
				Namespace:   "incrementalns",
				CursorField: "created_at",
			}})
			require.NoError(t, err)
			require.NotNil(t, res)
			require.Len(t, res, 1)
		}
	})
}
