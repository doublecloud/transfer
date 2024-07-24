package slices

import (
	"golang.org/x/exp/constraints"
)

// Subtract returns copy of slice a without elements of slice b.
func Subtract[T constraints.Ordered](a, b []T) []T {
	set := make(map[T]struct{})
	for _, elem := range b {
		set[elem] = struct{}{}
	}
	return Filter(a, func(elem T) bool {
		_, ok := set[elem]
		return !ok
	})
}
