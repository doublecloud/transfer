package poolba

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPoolOf(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		size := 10
		pool, err := PoolOf[net.Conn](size)
		assert.NoError(t, err)
		assert.IsType(t, &Pool[net.Conn]{}, pool)
		assert.Equal(t, size, cap(pool.hotResources))
		assert.Equal(t, size, cap(pool.coldResources))
	})

	t.Run("with_options", func(t *testing.T) {
		size := 1024
		constructor := func(ctx context.Context) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", "golang.org")
		}
		destructor := func(conn net.Conn) error {
			return conn.Close()
		}

		pool, err := PoolOf[net.Conn](size,
			WithConstructor(constructor),
			WithDestructor(destructor),
		)
		assert.NoError(t, err)
		assert.IsType(t, &Pool[net.Conn]{}, pool)

		assert.Equal(t, size, cap(pool.hotResources))
		assert.Equal(t, size, cap(pool.coldResources))
		assert.Equal(t, fmt.Sprintf("%p", constructor), fmt.Sprintf("%p", pool.constructor))
		assert.Equal(t, fmt.Sprintf("%p", destructor), fmt.Sprintf("%p", pool.destructor))
	})
}

func TestPool_Borrow(t *testing.T) {
	t.Run("error", func(t *testing.T) {
		var constructorCalls int32
		constructor := func(_ context.Context) (*bytes.Buffer, error) {
			atomic.AddInt32(&constructorCalls, 1)
			return nil, io.EOF
		}

		pool, err := PoolOf[*bytes.Buffer](10,
			WithConstructor(constructor),
		)
		assert.NoError(t, err)

		ctx := context.Background()
		res, err := pool.Borrow(ctx)
		assert.ErrorIs(t, err, io.EOF)
		assert.Nil(t, res)

		assert.Equal(t, int32(1), constructorCalls)
	})

	t.Run("context_timeout", func(t *testing.T) {
		constructor := func(_ context.Context) (*bytes.Buffer, error) {
			return nil, io.EOF
		}

		pool, err := PoolOf[*bytes.Buffer](10,
			WithConstructor(constructor),
		)
		assert.NoError(t, err)

		// emulate "no resources" situation
		for len(pool.coldResources) > 0 {
			<-pool.coldResources
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		res, err := pool.Borrow(ctx)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Nil(t, res)
	})

	t.Run("single", func(t *testing.T) {
		var constructorCalls int32
		constructor := func(_ context.Context) (*bytes.Buffer, error) {
			atomic.AddInt32(&constructorCalls, 1)
			return bytes.NewBufferString("default"), nil
		}

		pool, err := PoolOf[*bytes.Buffer](10,
			WithConstructor(constructor),
		)
		assert.NoError(t, err)

		ctx := context.Background()
		res, err := pool.Borrow(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.NotNil(t, res.value)
		assert.IsType(t, &bytes.Buffer{}, res.Value())

		assert.Equal(t, int32(1), constructorCalls)
		assert.Equal(t, "default", res.Value().String())
	})

	t.Run("reuse", func(t *testing.T) {
		var constructorCalls int32
		constructor := func(_ context.Context) (*bytes.Buffer, error) {
			atomic.AddInt32(&constructorCalls, 1)
			return bytes.NewBufferString("default"), nil
		}

		pool, err := PoolOf[*bytes.Buffer](10,
			WithConstructor(constructor),
		)
		assert.NoError(t, err)

		ctx := context.Background()

		res, err := pool.Borrow(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, res)
		res.Value().WriteString("ing")
		res.Vacay()

		res, err = pool.Borrow(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.Equal(t, "defaulting", res.Value().String())
	})

	t.Run("contention", func(t *testing.T) {
		contention := 1024

		var constructorCalls int32
		constructor := func(_ context.Context) (*bytes.Buffer, error) {
			atomic.AddInt32(&constructorCalls, 1)
			return bytes.NewBuffer(nil), nil
		}

		pool, err := PoolOf[*bytes.Buffer](contention,
			WithConstructor(constructor),
		)
		assert.NoError(t, err)

		ctx := context.Background()
		startChan := make(chan bool)

		var wg sync.WaitGroup
		wg.Add(contention)

		for i := 0; i < contention; i++ {
			go func() {
				<-startChan
				_, _ = pool.Borrow(ctx)
				wg.Done()
			}()
		}

		close(startChan)
		wg.Wait()

		assert.Equal(t, int32(contention), constructorCalls)
	})
}

func TestPool_Close(t *testing.T) {
	t.Run("greedy", func(t *testing.T) {
		size := 10
		constructor := func(_ context.Context) (*bytes.Buffer, error) {
			return bytes.NewBuffer(nil), nil
		}

		pool, err := PoolOf[*bytes.Buffer](size, WithConstructor(constructor))
		assert.NoError(t, err)

		ctx := context.Background()
		for i := 0; i < size; i++ {
			res, _ := pool.Borrow(ctx)
			res.Vacay()
			assert.NotNilf(t, res.Value(), "resource at address %d must not be nil", i)
		}

		err = pool.Close()
		assert.NoError(t, err)
		assert.Empty(t, pool.hotResources)
		assert.Equal(t, size, len(pool.coldResources))
	})

	t.Run("contention", func(t *testing.T) {
		var constructorCalls int32
		constructor := func(_ context.Context) (*bytes.Buffer, error) {
			atomic.AddInt32(&constructorCalls, 1)
			return bytes.NewBuffer(nil), nil
		}

		size := 256
		pool, err := PoolOf[*bytes.Buffer](size, WithConstructor(constructor))
		assert.NoError(t, err)

		contention := 2048
		ctx := context.Background()
		for i := 0; i < contention; i++ {
			go func() {
				for {
					res, err := pool.Borrow(ctx)
					if errors.Is(err, context.Canceled) {
						break
					}
					if err != nil {
						continue
					}
					// do work here
					time.Sleep(time.Millisecond)
					res.Vacay()
				}
			}()
		}

		time.Sleep(10 * time.Millisecond)
		_ = pool.Close()

		assert.Greater(t, constructorCalls, int32(0))
		assert.Empty(t, pool.hotResources)
		assert.Equal(t, size, len(pool.coldResources))
	})
}

func TestResource_Value(t *testing.T) {
	constructor := func(_ context.Context) (*bytes.Buffer, error) {
		return bytes.NewBuffer(nil), nil
	}

	pool, err := PoolOf[*bytes.Buffer](10,
		WithConstructor(constructor),
	)
	assert.NoError(t, err)

	res, err := pool.Borrow(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, res)

	assert.NotNil(t, res.value)
	assert.IsType(t, &bytes.Buffer{}, res.Value())
}

func TestResource_Vacay(t *testing.T) {
	t.Run("single", func(t *testing.T) {
		constructor := func(_ context.Context) (*bytes.Buffer, error) {
			return bytes.NewBuffer(nil), nil
		}

		pool, err := PoolOf[*bytes.Buffer](10,
			WithConstructor(constructor),
		)
		assert.NoError(t, err)

		res, err := pool.Borrow(context.Background())
		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.NotNil(t, res.value)

		res.Vacay()
		assert.NotNil(t, res.value)

		res.Vacay()
	})

	t.Run("contention", func(t *testing.T) {
		var constructorCalls int32
		constructor := func(_ context.Context) (*bytes.Buffer, error) {
			atomic.AddInt32(&constructorCalls, 1)
			return bytes.NewBuffer(nil), nil
		}

		size := 1024
		pool, err := PoolOf[*bytes.Buffer](size,
			WithConstructor(constructor),
		)
		assert.NoError(t, err)

		ctx := context.Background()
		startChan := make(chan bool)

		contention := 4096
		var wg sync.WaitGroup
		wg.Add(contention)

		for i := 0; i < contention; i++ {
			go func() {
				<-startChan
				res, _ := pool.Borrow(ctx)
				// do work
				time.Sleep(10 * time.Microsecond)
				res.Vacay()
				wg.Done()
			}()
		}

		close(startChan)
		wg.Wait()

		assert.LessOrEqual(t, int(constructorCalls), size)
	})
}

func TestResource_Close(t *testing.T) {
	var constructorCalls, destructorCalls int32
	constructor := func(_ context.Context) (*bytes.Buffer, error) {
		atomic.AddInt32(&constructorCalls, 1)
		return bytes.NewBuffer(nil), nil
	}
	destructor := func(buf *bytes.Buffer) error {
		atomic.AddInt32(&destructorCalls, 1)
		buf.Reset()
		return nil
	}

	pool, err := PoolOf[*bytes.Buffer](10,
		WithConstructor(constructor),
		WithDestructor(destructor),
	)
	assert.NoError(t, err)

	res, err := pool.Borrow(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.NotNil(t, res.value)
	res.Value().WriteString("changed")

	err = res.Close()
	assert.NoError(t, err)
	assert.Nil(t, res.value)

	assert.Equal(t, int32(1), constructorCalls)
	assert.Equal(t, int32(1), destructorCalls)
}
