package typefitting

import (
	"testing"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/middlewares"
	"github.com/doublecloud/transfer/pkg/providers/clickhouse"
	"github.com/doublecloud/transfer/tests/helpers"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/schema"
)

func TestTypeFitting(t *testing.T) {
	sch := abstract.NewTableSchema([]abstract.ColSchema{
		{
			TableName:  "test",
			ColumnName: "c_int16",
			DataType:   schema.TypeInt32.String(), // Downcast int32 -> int16
		},
		{
			TableName:  "test",
			ColumnName: "c_int32",
			DataType:   schema.TypeInt64.String(), // Downcast int64 -> int32
		},
		{
			TableName:  "test",
			ColumnName: "c_int64",
			DataType:   schema.TypeUint64.String(), // unsigned -> signed
		},
		{
			TableName:  "test",
			ColumnName: "c_uint32",
			DataType:   schema.TypeUint64.String(), // downcast uint64 -> uint32
		},
		{
			TableName:  "test",
			ColumnName: "c_uint64",
			DataType:   schema.TypeInt64.String(), // signed -> unsigned
		},
	})

	items := []abstract.ChangeItem{
		{
			Kind:         abstract.InsertKind,
			Table:        "test",
			ColumnNames:  []string{"c_int16", "c_int32", "c_int64", "c_uint32", "c_uint64"},
			ColumnValues: []any{int32(100), int64(100_000), uint64(100_000), uint64(100_000), int64(100_000)},
			TableSchema:  sch,
		},
	}

	transfer := helpers.MakeTransfer(helpers.TransferID, &source, &target, abstract.TransferTypeSnapshotOnly)
	sinker, err := clickhouse.NewSink(transfer, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), new(abstract.LocalRuntime), middlewares.MakeConfig())
	require.NoError(t, err)
	require.NoError(t, sinker.Push(items))
}
