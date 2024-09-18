package math

import "golang.org/x/exp/constraints"

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func MinT[T constraints.Ordered](a, b T) T {
	if a < b {
		return a
	}
	return b
}

func MaxT[T constraints.Ordered](a, b T) T {
	if a > b {
		return a
	}
	return b
}
