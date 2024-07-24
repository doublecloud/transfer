package poolba

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkPool_Borrow(b *testing.B) {
	constructor := func(_ context.Context) ([]byte, error) {
		return make([]byte, 8), nil
	}

	b.Run("with_reuse", func(b *testing.B) {
		size := 10000
		resources := make([]*Resource[[]byte], size)

		vacayAll := func(pool *Pool[[]byte]) {
			for _, res := range resources {
				res.Vacay()
			}
		}

		pool, err := PoolOf[[]byte](size, WithConstructor(constructor))
		require.NoError(b, err)

		ctx := context.Background()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if i > 0 && i%size == 0 {
				b.StopTimer()
				vacayAll(pool)
				b.StartTimer()
			}
			res, _ := pool.Borrow(ctx)
			resources[i%size] = res
		}
	})

	b.Run("without_reuse", func(b *testing.B) {
		size := 10000
		resources := make([]*Resource[[]byte], size)

		closeAll := func(pool *Pool[[]byte]) {
			for _, res := range resources {
				_ = res.Close()
			}
		}

		ctx := context.Background()

		pool, err := PoolOf[[]byte](size, WithConstructor(constructor))
		require.NoError(b, err)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if i > 0 && i%size == 0 {
				b.StopTimer()
				closeAll(pool)
				b.StartTimer()
			}
			res, _ := pool.Borrow(ctx)
			resources[i%size] = res
		}
	})
}
