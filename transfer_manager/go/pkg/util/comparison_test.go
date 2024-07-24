package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLess(t *testing.T) {
	require.False(t, Less())
	require.True(t, Less(NewComparator("a", "b")))
	require.False(t, Less(NewComparator("b", "a")))
	require.False(t, Less(NewComparator("a", "a")))
	require.True(t, Less(NewComparator("a", "a"), NewComparator(0, 1)))
	require.False(t, Less(NewComparator("a", "a"), NewComparator(1, 0)))
	require.False(t, Less(NewComparator("a", "a"), NewComparator(0, 0)))
	require.True(t, Less(NewComparator("a", "a"), NewComparator(0.1, 0.2)))
}
