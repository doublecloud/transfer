package ydb

import (
	"sync"
	"time"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/internal/stats"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/timeutil"
)

type connRuntime struct {
	mu           sync.RWMutex
	state        ConnState
	offlineCount uint64
	opStarted    uint64
	opSucceed    uint64
	opFailed     uint64
	opTime       *stats.Series
	opRate       *stats.Series
	errRate      *stats.Series
}

func (c *connRuntime) stats() ConnStats {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := timeutil.Now()

	r := ConnStats{
		State:        c.state,
		OpStarted:    c.opStarted,
		OpSucceed:    c.opSucceed,
		OpFailed:     c.opFailed,
		OpPerMinute:  c.opRate.SumPer(now, time.Minute),
		ErrPerMinute: c.errRate.SumPer(now, time.Minute),
	}
	if rtSum, rtCnt := c.opTime.Get(now); rtCnt > 0 {
		r.AvgOpTime = time.Duration(rtSum / float64(rtCnt))
	}

	return r
}

func (c *connRuntime) setState(s ConnState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state = s
	if s == ConnOffline {
		c.offlineCount++
	}
}

func (c *connRuntime) getState() (s ConnState) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

func (c *connRuntime) operationStart(start time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.opStarted++
	c.opRate.Add(start, 1)
}

func (c *connRuntime) operationDone(start, end time.Time, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err != nil {
		c.opFailed++
		c.errRate.Add(end, 1)
	} else {
		c.opSucceed++
	}
	c.opTime.Add(end, float64(end.Sub(start)))
}

func (c *connRuntime) streamStart(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.opRate.Add(now, 1)
}

func (c *connRuntime) streamRecv(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.opRate.Add(now, 1)
}

func (c *connRuntime) streamDone(now time.Time, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		c.errRate.Add(now, 1)
	}
}
