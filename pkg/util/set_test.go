package util

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetContains(t *testing.T) {
	s := NewSet[int](1, 3)
	require.True(t, s.Contains(1))
	require.False(t, s.Contains(2))
	require.True(t, s.Contains(3))

	s.Remove(1)
	require.False(t, s.Contains(1))

	s.Add(2)
	require.True(t, s.Contains(2))
}

func TestSetString(t *testing.T) {
	var empty []int
	require.Equal(t, fmt.Sprint(empty), fmt.Sprint(NewSet[int](empty...)))

	single := []int{1}
	require.Equal(t, fmt.Sprint(single), fmt.Sprint(NewSet[int](single...)))

	var multiple []string
	for _, permutation := range permutations([]int{1, 2, 3}) {
		multiple = append(multiple, fmt.Sprint(permutation))
	}
	require.Contains(t, multiple, fmt.Sprint(NewSet[int](1, 2, 3)))
}

func permutations[T any](items []T) [][]T {
	if len(items) == 0 {
		return nil
	}
	if len(items) == 1 {
		return [][]T{items}
	}
	var result [][]T
	for _, basePermutation := range permutations(items[1:]) {
		for i := range basePermutation {
			var permutation []T
			permutation = append(permutation, basePermutation[:i]...)
			permutation = append(permutation, items[0])
			permutation = append(permutation, basePermutation[i:]...)
			result = append(result, permutation)
		}
		basePermutation = append(basePermutation, items[0])
		result = append(result, basePermutation)
	}
	return result
}
