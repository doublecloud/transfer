package filter

import (
	"testing"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/stretchr/testify/require"
)

func TestSkipEvents(t *testing.T) {
	transformer, _ := NewSkipEvents(SkipEventsConfig{
		Tables: Tables{IncludeTables: []string{"table1"}},
		Events: []string{
			string(abstract.DeleteKind),
			string(abstract.TruncateTableKind),
			string(abstract.DropTableKind),
		},
	}, logger.Log)

	require.True(t, transformer.Suitable(*abstract.NewTableID("", "table1"), nil))
	require.False(t, transformer.Suitable(*abstract.NewTableID("", "table2"), nil))
	changeItems := []abstract.ChangeItem{
		{
			Kind:  abstract.DropTableKind,
			Table: "table1",
		},
		{
			Kind:  abstract.TruncateTableKind,
			Table: "table1",
		},
		{
			Kind:  abstract.InitTableLoad,
			Table: "table1",
		},
		{
			Kind:  abstract.DoneTableLoad,
			Table: "table1",
		},
		{
			Kind:  abstract.InsertKind,
			Table: "table1",
		},
		{
			Kind:  abstract.UpdateKind,
			Table: "table1",
		},
		{
			Kind:  abstract.DeleteKind,
			Table: "table1",
		},
	}
	result := transformer.Apply(changeItems)
	require.Empty(t, result.Errors)
	require.Equal(t, []abstract.ChangeItem{
		{
			Kind:  abstract.InitTableLoad,
			Table: "table1",
		},
		{
			Kind:  abstract.DoneTableLoad,
			Table: "table1",
		},
		{
			Kind:  abstract.InsertKind,
			Table: "table1",
		},
		{
			Kind:  abstract.UpdateKind,
			Table: "table1",
		},
	}, result.Transformed)
}
