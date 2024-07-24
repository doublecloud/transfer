package util

import (
	"fmt"
	"strings"

	"golang.org/x/exp/slices"
)

type Set[T comparable] struct {
	values map[T]struct{}
}

func NewSet[T comparable](values ...T) *Set[T] {
	result := &Set[T]{values: make(map[T]struct{})}
	result.AddRange(values...)
	return result
}

func (s *Set[T]) Add(value T) {
	s.values[value] = struct{}{}
}

func (s *Set[T]) AddRange(values ...T) {
	for _, value := range values {
		s.Add(value)
	}
}

func (s *Set[T]) Remove(value T) {
	delete(s.values, value)
}

func (s *Set[T]) RemoveRange(values ...T) {
	for _, value := range values {
		s.Remove(value)
	}
}

func (s *Set[T]) Len() int {
	return len(s.values)
}

func (s *Set[T]) Empty() bool {
	return s.Len() == 0
}

func (s *Set[T]) Contains(value T) bool {
	_, ok := s.values[value]
	return ok
}

func (s *Set[T]) Range(callback func(value T)) {
	for value := range s.values {
		callback(value)
	}
}

func (s *Set[T]) String() string {
	sb := strings.Builder{}
	sb.WriteString("[")
	first := true
	for value := range s.values {
		if !first {
			sb.WriteString(" ")
		} else {
			first = false
		}
		sb.WriteString(fmt.Sprint(value))
	}
	sb.WriteString("]")
	return sb.String()
}

func (s *Set[T]) Slice() []T {
	result := make([]T, 0, s.Len())
	for value := range s.values {
		result = append(result, value)
	}
	return result
}

func (s *Set[T]) SortedSliceFunc(less func(a, b T) bool) []T {
	result := s.Slice()
	slices.SortFunc(result, func(a, b T) int {
		if less(a, b) {
			return -1
		}
		return 1
	})
	return result
}

func (s *Set[T]) Difference(o *Set[T]) []T {
	// Although this method should have returned a set, it does not do that to avoid repeatable operations
	result := make([]T, 0)
	for value := range s.values {
		if !o.Contains(value) {
			result = append(result, value)
		}
	}
	return result
}

func (s *Set[T]) Equals(o *Set[T]) bool {
	return len(s.Difference(o)) == 0 && len(o.Difference(s)) == 0
}
