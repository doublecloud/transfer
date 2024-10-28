package async

import (
	"testing"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/metrics/nop"
	"github.com/doublecloud/transfer/pkg/abstract/changeitem"
	"github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/errors/coded"
	"github.com/doublecloud/transfer/pkg/providers"
	chrecipe "github.com/doublecloud/transfer/pkg/providers/clickhouse/recipe"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/schema"
)

type testCase struct {
	name         string
	item         changeitem.ChangeItem
	expectedCode coded.Code
}

var cases = []testCase{{
	name:         "BrokenDecimal",
	expectedCode: providers.DataValueError,
	item: changeitem.ChangeItem{
		Kind:         changeitem.InsertKind,
		Table:        "decimal",
		ColumnNames:  []string{"val"},
		ColumnValues: []any{"foo"},
		TableSchema: changeitem.NewTableSchema([]changeitem.ColSchema{{
			ColumnName: "val",
			DataType:   schema.TypeString.String(),
		}}),
	},
}, {
	name:         "BoolToDecimal",
	expectedCode: providers.UnsupportedConversion,
	item: changeitem.ChangeItem{
		Kind:         changeitem.InsertKind,
		Table:        "decimal",
		ColumnNames:  []string{"val"},
		ColumnValues: []any{true},
		TableSchema: changeitem.NewTableSchema([]changeitem.ColSchema{{
			ColumnName: "val",
			DataType:   schema.TypeBoolean.String(),
		}}),
	},
}}

var (
	target   = chrecipe.MustTarget(chrecipe.WithDatabase("test"), chrecipe.WithInitFile("gotest/errors_test_init.sql"))
	transfer = new(model.Transfer)
)

func TestCodedErrors(t *testing.T) {
	transfer.ID = "dttsample"
	target.Cleanup = model.DisabledCleanup
	for _, tc := range cases {
		t.Run(tc.name, testCodedError(tc))
	}
}

func testCodedError(tc testCase) func(t *testing.T) {
	return func(t *testing.T) {
		sink, err := NewSink(transfer, target, logger.Log, nop.Registry{}, nil)
		require.NoError(t, err)
		defer sink.Close()

		// in the sink. See comments in the sink itself.
		err = <-sink.AsyncPush([]changeitem.ChangeItem{{
			Kind:        changeitem.InitTableLoad,
			Schema:      tc.item.Schema,
			Table:       tc.item.Table,
			PartID:      tc.item.PartID,
			TableSchema: tc.item.TableSchema,
		}})
		require.NoError(t, err)

		err = <-sink.AsyncPush([]changeitem.ChangeItem{tc.item})
		var errCoded coded.CodedError
		require.ErrorAs(t, err, &errCoded)
		require.Equal(t, tc.expectedCode, errCoded.Code())
	}
}
