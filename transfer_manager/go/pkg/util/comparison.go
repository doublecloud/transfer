package util

import (
	"golang.org/x/exp/constraints"
)

type Comparable interface {
	// Return:
	// * 0 if values inside Comparable are equal
	// * a negative value if the left one is less than the right one
	// * a positive value if the left one is more than the right one
	Compare() int
}

type Comparator[T constraints.Ordered] struct {
	left  T
	right T
}

func (c Comparator[T]) Compare() int {
	if c.left < c.right {
		return -1
	}
	if c.left > c.right {
		return 1
	}
	return 0
}

func NewComparator[T constraints.Ordered](left, right T) *Comparator[T] {
	return &Comparator[T]{left: left, right: right}
}

func Less(chain ...Comparable) bool {
	for _, comparable := range chain {
		cmp := comparable.Compare()
		if cmp != 0 {
			return cmp < 0
		}
	}
	return false
}
