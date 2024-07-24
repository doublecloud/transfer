package lbenv

import (
	"context"
	"testing"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/recipe"
	"github.com/stretchr/testify/require"
)

type env struct {
	*recipe.Env

	t   *testing.T
	ctx context.Context
}

func (e *env) resetConsumerOffsets() {
	opts := e.ConsumerOptions()
	opts.ManualPartitionAssignment = true

	c := persqueue.NewReader(opts)

	_, err := c.Start(e.ctx)
	require.NoError(e.t, err)

	delay := time.After(time.Second)
	for {
		select {
		case m := <-c.C():
			switch l := m.(type) {
			case *persqueue.Lock:
				l.StartRead(false, l.EndOffset, l.EndOffset)
			}

		case <-delay:
			c.Shutdown()

			for range c.C() {
			}
			return
		}
	}
}

func NewLbEnv(t *testing.T) (e *env, stop func()) {
	e = &env{t: t}

	e.Env = recipe.New(t)

	var cancelCtx func()
	e.ctx, cancelCtx = context.WithTimeout(context.Background(), time.Minute)

	e.resetConsumerOffsets()

	stop = func() {
		cancelCtx()
	}

	return
}
