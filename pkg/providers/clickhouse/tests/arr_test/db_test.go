package arr_test_test

import (
	"testing"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/pkg/abstract"
	server "github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/middlewares"
	"github.com/doublecloud/transfer/pkg/providers/clickhouse"
	chrecipe "github.com/doublecloud/transfer/pkg/providers/clickhouse/recipe"
	"github.com/doublecloud/transfer/tests/helpers"
	"github.com/stretchr/testify/require"
)

var (
	Source = server.MockSource{}
	Target = *chrecipe.MustTarget(chrecipe.WithDatabase("test"), chrecipe.WithInitFile("init.sql"))
)

func TestCHArray(t *testing.T) {
	Source.WithDefaults()
	Target.WithDefaults()
	schema := abstract.NewTableSchema([]abstract.ColSchema{
		{
			TableName:    "test",
			ColumnName:   "arr",
			DataType:     "any",
			OriginalType: "ch:Array(UInt32)",
		},
		{
			TableName:    "test",
			ColumnName:   "arr_n",
			DataType:     "any",
			OriginalType: "ch:Array(Nullable(UInt32))",
		},
		{
			TableName:    "test",
			ColumnName:   "arr_arr_int",
			DataType:     "any",
			OriginalType: "ch:Array(Array(Int32))",
		},
		{
			TableName:    "test",
			ColumnName:   "arr_arr_str",
			DataType:     "any",
			OriginalType: "ch:Array(Array(String))",
		},
	})
	v1 := uint32(1)
	v2 := uint32(2)

	items := []abstract.ChangeItem{
		{
			Kind:        abstract.InsertKind,
			Table:       "test",
			ColumnNames: []string{"arr", "arr_n", "arr_arr_int", "arr_arr_str"},
			ColumnValues: []any{[]uint32{1, 2}, []any{uint32(3), uint32(4)},
				[][]any{{int32(1), int32(2)}, {int32(3)}},
				[][]any{{[]byte("foo"), "bar"}}},
			TableSchema: schema,
		},
		{
			Kind:         abstract.InsertKind,
			Table:        "test",
			ColumnNames:  []string{"arr", "arr_n", "arr_arr_int", "arr_arr_str"},
			ColumnValues: []any{[]any{&v1, &v2}, []any{uint32(1), nil}, []any{}, [][]any(nil)},
			TableSchema:  schema,
		},
	}

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotOnly)
	sinker, err := clickhouse.NewSink(transfer, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), new(abstract.LocalRuntime), middlewares.MakeConfig())
	require.NoError(t, err)
	require.NoError(t, sinker.Push(items))
}
