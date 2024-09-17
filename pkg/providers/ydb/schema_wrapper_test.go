package ydb

import (
	"testing"

	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/stretchr/testify/require"
)

func TestTableSchemaWrapper(t *testing.T) {
	tableSchema := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "a"},
	})

	currTableSchemaWrapper := newTableSchemaObj()
	currTableSchemaWrapper.Set(tableSchema)

	require.True(t, currTableSchemaWrapper.IsAllColumnNamesKnown(&cdcEvent{
		Update: map[string]interface{}{"a": 1},
	}))

	require.False(t, currTableSchemaWrapper.IsAllColumnNamesKnown(&cdcEvent{
		Update: map[string]interface{}{"a": 1, "b": 2},
	}))
	require.False(t, currTableSchemaWrapper.IsAllColumnNamesKnown(&cdcEvent{
		Update: map[string]interface{}{"b": 1},
	}))
}
