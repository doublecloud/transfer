package sharding

import (
	"context"
	_ "embed"
	"testing"

	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/providers/mysql"
	"github.com/doublecloud/transfer/pkg/providers/mysql/mysqlrecipe"
	default_mysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

//go:embed source.sql
var sourceDB []byte

func TestShardingByPartitions(t *testing.T) {
	source := mysqlrecipe.RecipeMysqlSource()
	if source.Database == "" {
		// init database
		source.Database = "source"
	}
	connectionParams, err := mysql.NewConnectionParams(source.ToStorageParams())
	require.NoError(t, err)
	db, err := mysql.Connect(connectionParams, func(config *default_mysql.Config) error {
		config.MultiStatements = true
		return nil
	})
	require.NoError(t, err)
	_, err = db.Exec(string(sourceDB))
	require.NoError(t, err)
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
