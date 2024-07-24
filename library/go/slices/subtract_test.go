package slices_test

import (
	"testing"

	"github.com/doublecloud/tross/library/go/slices"
	"github.com/stretchr/testify/require"
)

func TestSubtract(t *testing.T) {
	require.Equal(t, []int{3, 4, 5}, slices.Subtract([]int{1, 2, 3, 4, 5, 6}, []int{1, 2, 6, 7}))
	require.Equal(t, []int{1, 2, 3}, slices.Subtract([]int{1, 2, 3}, nil))
	require.Equal(t, []int{1, 2, 3}, slices.Subtract([]int{1, 2, 3}, []int{}))
	require.Equal(t, []int{1, 2, 3}, slices.Subtract([]int{1, 2, 3}, []int{4, 5, 6}))
	require.Equal(t, []int{}, slices.Subtract([]int{1, 2, 3}, []int{1, 2, 3}))
}
