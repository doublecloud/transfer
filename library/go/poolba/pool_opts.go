package poolba

type PoolOpt[T any] func(pool *Pool[T]) error

func WithConstructor[T any](fn ConstructorFunc[T]) PoolOpt[T] {
	return func(pool *Pool[T]) error {
		pool.constructor = fn
		return nil
	}
}

func WithDestructor[T any](fn DestructorFunc[T]) PoolOpt[T] {
	return func(pool *Pool[T]) error {
		pool.destructor = fn
		return nil
	}
}
