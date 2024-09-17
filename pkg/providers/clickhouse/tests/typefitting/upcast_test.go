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

func TestTypeUpcast(t *testing.T) {
	source.WithDefaults()
	target.WithDefaults()
	sch := abstract.NewTableSchema([]abstract.ColSchema{
		{
			TableName:  "test",
			ColumnName: "c_int16",
			DataType:   schema.TypeInt8.String(),
		},
		{
			TableName:  "test",
			ColumnName: "c_int32",
			DataType:   schema.TypeInt16.String(),
		},
		{
			TableName:  "test",
			ColumnName: "c_int64",
			DataType:   schema.TypeInt32.String(),
		},
		{
			TableName:  "test",
			ColumnName: "c_uint32",
			DataType:   schema.TypeUint16.String(),
		},
		{
			TableName:  "test",
			ColumnName: "c_uint64",
			DataType:   schema.TypeUint32.String(),
		},
	})

	items := []abstract.ChangeItem{
		{
			Kind:         abstract.InsertKind,
			Table:        "test",
			ColumnNames:  []string{"c_int16", "c_int32", "c_int64", "c_uint32", "c_uint64"},
			ColumnValues: []any{int8(100), int16(10000), int32(100_000), uint16(10_000), uint32(100_000)},
			TableSchema:  sch,
		},
	}

	transfer := helpers.MakeTransfer(helpers.TransferID, &source, &target, abstract.TransferTypeSnapshotOnly)
	sinker, err := clickhouse.NewSink(transfer, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), new(abstract.LocalRuntime), middlewares.MakeConfig())
	require.NoError(t, err)
	require.NoError(t, sinker.Push(items))
}
