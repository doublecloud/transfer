package test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/recipe"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/session"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

type env struct {
	*recipe.Env

	t   *testing.T
	ctx context.Context
}

func newEnv(t *testing.T) (e *env, stop func()) {
	e = &env{t: t}

	e.Env = recipe.New(t)

	var cancelCtx func()
	e.ctx, cancelCtx = context.WithTimeout(context.Background(), time.Minute)

	e.resetConsumerOffsets()

	stop = func() {
		cancelCtx()

		goleak.VerifyNone(t)
	}

	return
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

func withFailingConnections(t *testing.T) (started chan struct{}, reset func()) {
	started = make(chan struct{})
	stopped := make(chan struct{})

	var wg sync.WaitGroup
	hook := func(session *session.Session) {
		wg.Add(1)
		go func() {
			defer wg.Done()

			select {
			case <-started:
				select {
				case <-stopped:

				case <-time.After(time.Second):
					t.Log("Injecting connection failure")
					_ = session.Close()
				}

			case <-stopped:
			}
		}()
	}

	persqueue.InternalSessionHook = hook

	reset = func() {
		persqueue.InternalSessionHook = func(*session.Session) {}

		close(stopped)
		wg.Wait()
	}

	return
}
