package broadcast

import (
	"context"
	"sync"
	"time"
)

// EventBroadcast is implementation of broadcast notify about event
// Zero value is usable, must not copy after first call any method
type EventBroadcast struct {
	m sync.Mutex

	nextEvent      context.Context
	nextEventClose context.CancelFunc
}

func (b *EventBroadcast) initNeedLock() {
	if b.nextEvent == nil {
		b.nextEvent, b.nextEventClose = context.WithCancel(context.Background())
	}
}

// Waiter return channel, that will close when next event will be broadcast.
// For prevent race between subscribe and event client code must subscribe at first, then check condition
// if false - wait closing channed and check condition again
func (b *EventBroadcast) Waiter() OneTimeWaiter {
	b.m.Lock()
	defer b.m.Unlock()

	b.initNeedLock()

	return OneTimeWaiter{b.nextEvent}
}

func (b *EventBroadcast) Notify() {
	b.m.Lock()
	defer b.m.Unlock()

	b.initNeedLock()

	b.nextEventClose()
	b.nextEvent, b.nextEventClose = context.WithCancel(context.Background())
}

type OneTimeWaiter struct {
	ctx context.Context
}

func (w OneTimeWaiter) Deadline() (deadline time.Time, ok bool) {
	return w.ctx.Deadline()
}

func (w OneTimeWaiter) Err() error {
	return w.ctx.Err()
}

func (w OneTimeWaiter) Value(key any) any {
	return w.ctx.Value(key)
}

// Done return channel, that closed by call notify on notifier
func (w OneTimeWaiter) Done() <-chan struct{} {
	return w.ctx.Done()
}

// Wait - wait event with ctx for timeout. Return nil if event notified before ctx closed.
func (w OneTimeWaiter) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.ctx.Done():
		return nil
	}
}
