package sharding

import (
	"context"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/providers/mysql"
	"github.com/doublecloud/transfer/pkg/providers/mysql/mysqlrecipe"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestShardingByPartitions(t *testing.T) {
	source := mysqlrecipe.RecipeMysqlSource()
	storage, err := mysql.NewStorage(source.ToStorageParams())
	require.NoError(t, err)
	parts, err := storage.ShardTable(context.Background(), abstract.TableDescription{
		Name:   "orders",
		Schema: source.Database,
		Filter: "",
		EtaRow: 0,
		Offset: 0,
	})
	require.NoError(t, err)
	require.Len(t, parts, 4)
	resRows := 0
	for _, part := range parts {
		require.NoError(
			t,
			storage.LoadTable(context.Background(), part, func(items []abstract.ChangeItem) error {
				for _, r := range items {
					if r.IsRowEvent() {
						resRows++
					}
				}
				return nil
			}),
		)
	}
	require.Equal(t, resRows, 6)
}
