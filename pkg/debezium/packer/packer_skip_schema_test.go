package packer

import (
	"testing"

	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/stretchr/testify/require"
)

func TestPackerSkipSchema(t *testing.T) {
	packerSkipSchema := NewPackerSkipSchema()
	result, err := packerSkipSchema.Pack(
		getTestChangeItem(),
		func(changeItem *abstract.ChangeItem) ([]byte, error) { return []byte{1}, nil }, // payload
		func(changeItem *abstract.ChangeItem) ([]byte, error) { return []byte{2}, nil }, // schema
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, result)
}
