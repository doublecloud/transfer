package ydb

import (
	"context"
	"sync"
	"time"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/timeutil"
)

// repeater contains logic of repeating some task.
type repeater struct {
	// Interval contains an interval between task execution.
	// Interval must be greater than zero; if not, Repeater will panic.
	interval time.Duration

	// Timeout for an operation passed as a context instance.
	// If 0 passed - no timeout is set
	timeout time.Duration

	// Task is a function that must be executed periodically.
	task func(context.Context)

	timer    timeutil.Timer
	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
	ctx      context.Context
	cancel   context.CancelFunc
	force    chan struct{}
}

type repeaterOptionsHolder struct {
	ctx context.Context
}

type repeaterOption func(h *repeaterOptionsHolder)

func withRepeaterContext(ctx context.Context) repeaterOption {
	return func(h *repeaterOptionsHolder) {
		h.ctx = ctx
	}
}

// NewRepeater creates and begins to execute task periodically.
func NewRepeater(interval, timeout time.Duration, task func(ctx context.Context), opts ...repeaterOption) *repeater {
	if interval <= 0 {
		return nil
	}
	h := &repeaterOptionsHolder{
		ctx: context.Background(),
	}
	for _, o := range opts {
		o(h)
	}
	ctx, cancel := context.WithCancel(h.ctx)
	r := &repeater{
		interval: interval,
		timeout:  timeout,
		task:     task,
		timer:    timeutil.NewTimer(interval),
		stopOnce: sync.Once{},
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
		ctx:      ctx,
		cancel:   cancel,
		force:    make(chan struct{}),
	}
	go r.worker()
	return r
}

// Stop stops to execute its task.
func (r *repeater) Stop() {
	r.stopOnce.Do(func() {
		close(r.stop)
		r.cancel()
		<-r.done
	})
}

func (r *repeater) Force() {
	select {
	case r.force <- struct{}{}:
	default:
	}
}

func (r *repeater) worker() {
	defer func() {
		r.timer.Stop()
		close(r.done)
	}()
	for {
		select {
		case <-r.stop:
			return
		case <-r.timer.C():
			// pass
		case <-r.force:
			// pass
		}
		r.timer.Reset(r.interval)
		ctx := r.ctx
		var cancel context.CancelFunc
		if t := r.timeout; t > 0 {
			ctx, cancel = context.WithTimeout(ctx, t)
		} else {
			ctx, cancel = context.WithCancel(ctx)
		}
		r.task(ctx)
		cancel()
	}
}
