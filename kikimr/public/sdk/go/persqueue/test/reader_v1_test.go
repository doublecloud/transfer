package test

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue"
	"github.com/stretchr/testify/require"
)

func TestReadSingleMessageV1(t *testing.T) {
	env, stop := newEnv(t)
	defer stop()

	require.NoError(t, writeMsgs(env, [][]byte{[]byte("ping")}))
	cfg := env.ConsumerOptions()
	cfg.RetryOnFailure = false
	c := persqueue.NewReaderV1(cfg)

	_, err := c.Start(env.ctx)
	require.NoError(t, err)

	for e := range c.C() {
		switch m := e.(type) {
		case *persqueue.LockV1:
			m.StartRead(false, m.ReadOffset, m.ReadOffset)
		case *persqueue.Data:
			for _, b := range m.Batches() {
				for _, r := range b.Messages {
					log.Printf("received data: %v", string(r.Data))
				}
			}
			m.Commit()
			c.Shutdown()

		case *persqueue.CommitAck:
			log.Printf("received data commit ack: %v (%v)", m.Cookies, m.PartitionCookies)
		default:
			t.Fatalf("Received unexpected event %T", m)
		}
	}

	require.NoError(t, c.Err())
}

func TestCancelReaderContextV1(t *testing.T) {
	env, stop := newEnv(t)
	defer stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := persqueue.NewReaderV1(env.ConsumerOptions())

	_, err := c.Start(ctx)
	require.NoError(t, err)

	time.Sleep(time.Millisecond * 50)
}

func TestReaderRestartV1(t *testing.T) {
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

func TestReadWithManualPartitionAssignmentV1(t *testing.T) {
	env, stop := newEnv(t)
	defer stop()

	require.NoError(t, writeMsgs(env, [][]byte{[]byte("ping")}))

	opts := env.ConsumerOptions()
	opts.ManualPartitionAssignment = true
	c := persqueue.NewReaderV1(opts)

	_, err := c.Start(env.ctx)
	require.NoError(t, err)

	for e := range c.C() {
		switch m := e.(type) {
		case *persqueue.LockV1:
			m.StartRead(true, m.ReadOffset, m.ReadOffset)

		case *persqueue.Data:
			m.Commit()
			c.Shutdown()
		}
	}

	require.NoError(t, c.Err())
}
