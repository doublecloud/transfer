package filter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFilter(t *testing.T) {
	emptyFilter, err := NewFilter(nil, nil)
	require.NoError(t, err)

	require.True(t, emptyFilter.Match("any_value"))

	excludeFilter, err := NewFilter(nil, []string{"exclude"})
	require.NoError(t, err)
	require.True(t, excludeFilter.Match("include"))
	require.False(t, excludeFilter.Match("exclude"))

	includeFilter, err := NewFilter([]string{"include"}, []string{"exclude"})
	require.NoError(t, err)
	require.True(t, includeFilter.Match("include"))
	require.False(t, includeFilter.Match("exclude"))
	require.False(t, includeFilter.Match("other"))

	excludePriorityFilter, err := NewFilter([]string{"include", "other.*"}, []string{".*other.*"})
	require.NoError(t, err)
	require.True(t, excludePriorityFilter.Match("include"))
	require.False(t, excludePriorityFilter.Match("other"))

	_, err = NewFilter([]string{"include", "*"}, []string{".*other.*"})
	require.Error(t, err)
}
