package slices

import (
	"cmp"
	"slices"
)

// Sort is like slices.Sort but returns sorted copy of given slice
func Sort[T cmp.Ordered](s []T) []T {
	s2 := make([]T, len(s))
	copy(s2, s)
	slices.Sort(s2)
	return s2
}
