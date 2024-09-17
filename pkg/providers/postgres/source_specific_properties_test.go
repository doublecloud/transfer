package postgres

import (
	"testing"

	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/stretchr/testify/require"
)

func TestEnumAllValues(t *testing.T) {
	currColSchema := &abstract.ColSchema{
		Properties: map[abstract.PropertyKey]interface{}{EnumAllValues: []string{"a", "b"}},
	}
	arr := GetPropertyEnumAllValues(currColSchema)
	require.NotNil(t, arr)
	require.Len(t, arr, 2)
}
