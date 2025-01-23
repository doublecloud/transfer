package util

// NewIfNil returns specified ptr, initializing it with new object if ptr is nil.
func NewIfNil[T any](ptr **T) *T {
	if *ptr == nil {
		*ptr = new(T)
	}
	return *ptr
}
