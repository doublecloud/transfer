package poolba

import (
	"context"
	"errors"
	"fmt"
)

var errPoolClosed = errors.New("pool closed")

// ConstructorFunc will be called for any uninitialized resource upon fetch from pool.
type ConstructorFunc[T any] func(context.Context) (T, error)

// DestructorFunc will be called for any closed resource before it will be returned to pool.
type DestructorFunc[T any] func(T) error

// Pool represents thread-safe generic resource pool of fixed size.
type Pool[T any] struct {
	// cancel context to be used on pool close
	closeCtx  context.Context
	closeFunc context.CancelFunc

	constructor ConstructorFunc[T]
	destructor  DestructorFunc[T]

	// hotResources contains already initialized and ready to be used resources.
	// coldResources contains only uninitialized resources.
	// Both hotResources and coldResources channel contain exactly "size" of resources combined
	hotResources  chan *Resource[T]
	coldResources chan *Resource[T]
}

// PoolOf returns new generic resource pool of given size.
func PoolOf[T any](size int, opts ...PoolOpt[T]) (*Pool[T], error) {
	ctx, cancel := context.WithCancel(context.Background())

	// create pool with sensible defaults
	pool := &Pool[T]{
		closeCtx:  ctx,
		closeFunc: cancel,
		// constructor simply returns zero value of T by default
		constructor: func(_ context.Context) (T, error) {
			var nr T
			return nr, nil
		},
		// no-op destructor by default
		destructor: func(_ T) error {
			return nil
		},
		hotResources:  make(chan *Resource[T], size),
		coldResources: make(chan *Resource[T], size),
	}

	// apply functional options
	for _, opt := range opts {
		if err := opt(pool); err != nil {
			return nil, fmt.Errorf("cannot apply option to pool: %w", err)
		}
	}

	// populate corresponding channel with uninitialized resources
	for i := 0; i < size; i++ {
		pool.coldResources <- &Resource[T]{pool: pool}
	}

	return pool, nil
}

// Borrow withdraws resource from pool.
// Result resource cannot be acquired by another thread,
// so it must be eventually returned back to pool via Vacay method.
// This method will block code execution until resource is available,
// given context canceled or pool closed.
func (p *Pool[T]) Borrow(ctx context.Context) (*Resource[T], error) {
	// checking hot resources first and moving forward if none available
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.closeCtx.Done():
		return nil, p.closeCtx.Err()
	case res, ok := <-p.hotResources:
		if !ok {
			return nil, errPoolClosed
		}
		return res, nil
	default:
		break
	}

	// waiting for hot or cold resource to be available
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.closeCtx.Done():
		return nil, p.closeCtx.Err()
	case res, ok := <-p.hotResources:
		if !ok {
			return nil, errPoolClosed
		}
		return res, nil
	case res, ok := <-p.coldResources:
		if !ok {
			return nil, errPoolClosed
		}
		// initializing cold resource
		val, err := p.constructor(ctx)
		if err != nil {
			p.coldResources <- res
			return nil, err
		}
		res.value = &val
		return res, nil
	}
}

// Close closes pool entirely and destructs all initialized resources.
// Pool cannot be used after close.
func (p *Pool[T]) Close() (err error) {
	p.closeFunc()

	// withdraw and close all hot resources
	for res := range p.hotResources {
		if derr := res.Close(); derr != nil {
			err = derr
		}
		if len(p.coldResources) == cap(p.coldResources) {
			break
		}
	}

	// close resource channels to release resources
	close(p.hotResources)
	close(p.coldResources)
	return
}

// Resource represents generic reusable resource.
type Resource[T any] struct {
	value *T
	pool  *Pool[T]
}

// Value returns actual user defined generic resource.
func (r *Resource[T]) Value() T {
	return *r.value
}

// Vacay returns resource back to pool.
// It can be reused without initialization later by another thread.
// Be aware that resource can be vacated exactly once
// to avoid data race conditions and resource leaks.
func (r *Resource[T]) Vacay() {
	r.pool.hotResources <- r
}

// Close deinitializes resource and returns it back to pool.
// Be aware that resource can to closed exactly once
// to avoid data race conditions and resource leaks.
func (r *Resource[T]) Close() error {
	err := r.pool.destructor(*r.value)
	r.value = nil
	r.pool.coldResources <- r
	return err
}
