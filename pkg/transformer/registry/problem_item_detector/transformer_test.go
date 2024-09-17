package problemitemdetector

import (
	"testing"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/stretchr/testify/require"
)

func TestTransformer(t *testing.T) {
	transformer := &problemItemDetector{Config{}, logger.Log}
	require.False(t, transformer.Suitable(abstract.TableID{}, nil))
	require.Equal(t, "problem item detector", transformer.Description())

	changeItems := []abstract.ChangeItem{{}, {Table: "table"}}
	expected := abstract.TransformerResult{
		Transformed: changeItems,
		Errors:      nil,
	}
	require.Equal(t, expected, transformer.Apply(changeItems))

	tableSchema := &abstract.TableSchema{}

	schema, err := transformer.ResultSchema(tableSchema)
	require.NoError(t, err)
	require.Equal(t, tableSchema, schema)
}
