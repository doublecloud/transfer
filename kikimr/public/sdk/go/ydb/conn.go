package ydb

import (
	"context"
	"sync"
	"time"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/internal/stats"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

type conn struct {
	addr connAddr
	dial func(ctx context.Context, address string) (*grpc.ClientConn, error)

	m  sync.RWMutex
	cc *grpc.ClientConn

	runtime connRuntime
}

// Deprecated: will be removed at next major release
func (c *conn) Conn() *conn {
	return c
}

func (c *conn) isBroken() bool {
	if c == nil {
		return true
	}
	c.m.RLock()
	defer c.m.RUnlock()
	return c._isBroken()
}

func (c *conn) _isBroken() bool {
	if c.cc == nil {
		return true
	}
	switch c.cc.GetState() {
	case connectivity.Shutdown, connectivity.TransientFailure:
		return true
	default:
		return false
	}
}

func (c *conn) isReady() bool {
	if c == nil {
		return false
	}
	c.m.RLock()
	defer c.m.RUnlock()
	return c._isReady()
}

func (c *conn) _isReady() bool {
	if c.cc == nil {
		return false
	}
	switch c.cc.GetState() {
	case connectivity.Ready:
		return true
	default:
		return false
	}
}

func (c *conn) close() error {
	if c == nil {
		return nil
	}
	c.m.Lock()
	defer c.m.Unlock()
	if c.cc == nil {
		return nil
	}
	return c.cc.Close()
}

func (c *conn) _close() error {
	if c.cc == nil {
		return nil
	}
	return c.cc.Close()
}

func (c *conn) getConn(ctx context.Context) (_ *grpc.ClientConn, err error) {
	c.m.Lock()
	defer c.m.Unlock()
	if c._isBroken() {
		_ = c._close()
		c.cc, err = c.dial(ctx, c.addr.String())
	}
	return c.cc, err
}

func (c *conn) Address() string {
	if c == nil {
		return ""
	}
	return c.addr.String()
}

func newConn(addr connAddr, dial func(ctx context.Context, address string) (*grpc.ClientConn, error)) *conn {
	const (
		statsDuration = time.Minute
		statsBuckets  = 12
	)
	return &conn{
		dial: dial,
		addr: addr,
		runtime: connRuntime{
			state:   ConnOffline,
			opTime:  stats.NewSeries(statsDuration, statsBuckets),
			opRate:  stats.NewSeries(statsDuration, statsBuckets),
			errRate: stats.NewSeries(statsDuration, statsBuckets),
		},
	}
}
