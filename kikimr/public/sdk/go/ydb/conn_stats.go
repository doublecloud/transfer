package ydb

import "time"

type ConnStats struct {
	State        ConnState
	OpStarted    uint64
	OpFailed     uint64
	OpSucceed    uint64
	OpPerMinute  float64
	ErrPerMinute float64
	AvgOpTime    time.Duration
}

func ReadConnStats(d Driver, f func(Endpoint, ConnStats)) {
	x, ok := d.(*driver)
	if !ok {
		return
	}
	x.cluster.Stats(f)
}

func (c ConnStats) OpPending() uint64 {
	return c.OpStarted - (c.OpFailed + c.OpSucceed)
}
