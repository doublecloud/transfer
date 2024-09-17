package helpers

import (
	"testing"

	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/stretchr/testify/require"
)

type TableSchema struct {
	nameToColSchema map[string]abstract.ColSchema
}

func (t *TableSchema) NameToTableSchema(tt *testing.T, colName string) *abstract.ColSchema {
	result, ok := t.nameToColSchema[colName]
	require.True(tt, ok)
	return &result
}

func MakeTableSchema(changeItem *abstract.ChangeItem) *TableSchema {
	mapColNameToIndex := make(map[string]abstract.ColSchema)
	for _, col := range changeItem.TableSchema.Columns() {
		mapColNameToIndex[col.ColumnName] = col
	}
	return &TableSchema{
		nameToColSchema: mapColNameToIndex,
	}
}
