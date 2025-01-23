package util

import (
	"sort"

	"golang.org/x/exp/constraints"
)

// MapKeysInOrder returns a sorted slice of all the keys in the given map.
func MapKeysInOrder[K constraints.Ordered, V any](m map[K]V) []K {
	result := make([]K, 0, len(m))
	for k := range m {
		result = append(result, k)
	}
	sort.SliceStable(result, func(i, j int) bool {
		return result[i] < result[j]
	})
	return result
}
