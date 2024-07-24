package postgres

import (
	"testing"

	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/stretchr/testify/require"
)

func TestLastFullLSN(t *testing.T) {
	t.Run("regular push with multiple tx", func(t *testing.T) {
		changes := []abstract.ChangeItem{
			{ID: 1, LSN: 1},
			{ID: 1, LSN: 2},
			{ID: 1, LSN: 3},
			{ID: 1, LSN: 4},
			{ID: 2, LSN: 5},
			{ID: 2, LSN: 6},
			{ID: 3, LSN: 7},
			{ID: 3, LSN: 8},
		}
		maxLsn := lastFullLSN(changes, 0, 0, 0)
		require.Equal(t, uint64(6), maxLsn)
	})
	t.Run("single tx push", func(t *testing.T) {
		changes := []abstract.ChangeItem{
			{ID: 1, LSN: 1}, {ID: 1, LSN: 2}, {ID: 1, LSN: 3}, {ID: 1, LSN: 4},
		}
		maxLsn := lastFullLSN(changes, 1, 0, 0)
		require.Equal(t, uint64(0), maxLsn)
	})
	t.Run("many single-tx push", func(t *testing.T) {
		changes1 := []abstract.ChangeItem{{ID: 1, LSN: 1}}
		changes2 := []abstract.ChangeItem{{ID: 2, LSN: 2}}
		changes3 := []abstract.ChangeItem{{ID: 3, LSN: 3}}
		maxLsn := lastFullLSN(changes1, 0, 0, 0)
		require.Equal(t, uint64(0), maxLsn)
		maxLsn = lastFullLSN(changes2, 1, 1, maxLsn)
		require.Equal(t, uint64(1), maxLsn)
		maxLsn = lastFullLSN(changes3, 2, 2, maxLsn)
		require.Equal(t, uint64(2), maxLsn)
	})
	t.Run("mixed-tx push", func(t *testing.T) {
		changes1 := []abstract.ChangeItem{{ID: 1, LSN: 1}}
		changes2 := []abstract.ChangeItem{{ID: 1, LSN: 2}}
		changes3 := []abstract.ChangeItem{{ID: 2, LSN: 3}}
		maxLsn := lastFullLSN(changes1, 0, 0, 0)
		require.Equal(t, uint64(0), maxLsn)
		maxLsn = lastFullLSN(changes2, 1, 1, maxLsn)
		require.Equal(t, uint64(0), maxLsn)
		maxLsn = lastFullLSN(changes3, 1, 2, maxLsn)
		require.Equal(t, uint64(2), maxLsn)
	})
}

func TestWal2jsonTableFromTableID(t *testing.T) {
	t.Run("wal2jsonfti_schema_table", func(t *testing.T) {
		require.Equal(
			t,
			`schema.table`,
			wal2jsonTableFromTableID(*abstract.NewTableID("schema", "table")),
		)
	})

	t.Run("wal2jsonfti_noschema_table", func(t *testing.T) {
		require.Equal(
			t,
			`*.table`,
			wal2jsonTableFromTableID(*abstract.NewTableID("", "table")),
		)
	})

	t.Run("wal2jsonfti_schema_star", func(t *testing.T) {
		require.Equal(
			t,
			`schema.*`,
			wal2jsonTableFromTableID(*abstract.NewTableID("schema", "*")),
		)
	})

	t.Run("wal2jsonfti_noschema_star", func(t *testing.T) {
		require.Equal(
			t,
			`*.*`,
			wal2jsonTableFromTableID(*abstract.NewTableID("", "*")),
		)
	})

	t.Run("wal2jsonfti_schema_tablewithdots", func(t *testing.T) {
		require.Equal(
			t,
			`schema.tab\.l\.e`,
			wal2jsonTableFromTableID(*abstract.NewTableID("schema", "tab.l.e")),
		)
	})

	t.Run("wal2jsonfti_schemawithdots_table", func(t *testing.T) {
		require.Equal(
			t,
			`sche\.ma\..table`,
			wal2jsonTableFromTableID(*abstract.NewTableID("sche.ma.", "table")),
		)
	})

	t.Run("wal2jsonfti_schemawithdots_notable", func(t *testing.T) {
		require.Equal(
			t,
			`sche\.ma\..*`,
			wal2jsonTableFromTableID(*abstract.NewTableID("sche.ma.", "")),
		)
	})
}
