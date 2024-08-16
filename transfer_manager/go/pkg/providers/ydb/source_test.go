package ydb

import (
	"context"
	"testing"

	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	mockstorage "github.com/doublecloud/transfer/transfer_manager/go/tests/helpers/mock_storage"
	"github.com/stretchr/testify/require"
)

func TestGetUpToDateTableSchema(t *testing.T) {
	// prepare

	tableSchema0 := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "a"},
	})
	currSchemaWrapper := newSchemaObj()
	currSchemaWrapper.Set("tableName0", tableSchema0)

	newTableSchema0 := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "a"},
		{ColumnName: "b"},
	})

	var tableNameAskedSchema *string = nil

	mockedStorage := mockstorage.NewMockStorage()
	mockedStorage.TableSchemaF = func(ctx context.Context, table abstract.TableID) (*abstract.TableSchema, error) {
		tableNameAskedSchema = new(string)
		*tableNameAskedSchema = table.Name
		return newTableSchema0, nil
	}

	src := &Source{
		cfg:     &YdbSource{},
		storage: mockedStorage,
	}
	src.schema = currSchemaWrapper

	// all columns are known

	ev0 := &cdcEvent{
		Update: map[string]interface{}{"a": 1},
	}
	_, err := src.getUpToDateTableSchema("tableName0", ev0)
	require.NoError(t, err)
	require.Nil(t, tableNameAskedSchema)

	// one new column

	ev1 := &cdcEvent{
		Update: map[string]interface{}{"a": 1, "b": 2},
	}
	_, err = src.getUpToDateTableSchema("tableName0", ev1)
	require.NoError(t, err)
	require.Equal(t, "tableName0", *tableNameAskedSchema)
}
