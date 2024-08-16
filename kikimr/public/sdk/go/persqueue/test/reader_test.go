package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue"
	"github.com/stretchr/testify/require"
)

func writeMsgs(e *env, msgs [][]byte) error {
	p := persqueue.NewWriter(e.ProducerOptions())
	_, err := p.Init(e.ctx)
	if err != nil {
		return err
	}

	for _, msg := range msgs {
		if err = p.Write(&persqueue.WriteMessage{Data: msg}); err != nil {
			return err
		}
	}

	return p.Close()
}

func TestReadSingleMessage(t *testing.T) {
	env, stop := newEnv(t)
	defer stop()

	require.NoError(t, writeMsgs(env, [][]byte{[]byte("ping")}))

	c := persqueue.NewReader(env.ConsumerOptions())

	_, err := c.Start(env.ctx)
	require.NoError(t, err)

	for e := range c.C() {
		switch m := e.(type) {
		case *persqueue.Data:
			m.Commit()
			c.Shutdown()

		case *persqueue.CommitAck:

		default:
			t.Fatalf("Received unexpected event %T", m)
		}
	}

	require.NoError(t, c.Err())
}

func TestCancelReaderContext(t *testing.T) {
	env, stop := newEnv(t)
	defer stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := persqueue.NewReader(env.ConsumerOptions())

	_, err := c.Start(ctx)
	require.NoError(t, err)

	time.Sleep(time.Millisecond * 50)
}

func TestReaderRestart(t *testing.T) {
	env, stop := newEnv(t)
	defer stop()

	var pings [][]byte
	for i := 0; i < 1000; i++ {
		pings = append(pings, []byte(fmt.Sprintf("ping%d", i)))
	}

	require.NoError(t, writeMsgs(env, pings))

	opts := env.ConsumerOptions()
	opts.MaxReadMessagesCount = 1
	c := persqueue.NewReader(opts)

	started, reset := withFailingConnections(t)
	defer reset()

	_, err := c.Start(env.ctx)
	require.NoError(t, err)

	close(started)

	restarted := 0
	for e := range c.C() {
		switch m := e.(type) {
		case *persqueue.Data:
			m.Commit()

			if restarted > 5 {
				c.Shutdown()
			}

		case *persqueue.Disconnect:
			restarted++
		}
	}
}

func TestReadWithManualPartitionAssignment(t *testing.T) {
	env, stop := newEnv(t)
	defer stop()

	require.NoError(t, writeMsgs(env, [][]byte{[]byte("ping")}))

	opts := env.ConsumerOptions()
	opts.ManualPartitionAssignment = true
	c := persqueue.NewReader(opts)

	_, err := c.Start(env.ctx)
	require.NoError(t, err)

	for e := range c.C() {
		switch m := e.(type) {
		case *persqueue.Lock:
			m.StartRead(true, m.ReadOffset, m.ReadOffset)

		case *persqueue.Data:
			m.Commit()
			c.Shutdown()
		}
	}

	require.NoError(t, c.Err())
}
