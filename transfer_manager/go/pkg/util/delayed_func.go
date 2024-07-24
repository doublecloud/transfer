package util

import (
	"sync/atomic"
	"time"
)

type delayedFunc struct {
	fn func()

	timer    *time.Timer
	canceled int32
}

// DelayFunc delays the execution of the given func by the given duration. The execution can be cancelled.
func DelayFunc(fn func(), d time.Duration) *delayedFunc {
	result := &delayedFunc{
		fn: fn,

		timer:    time.NewTimer(d),
		canceled: 0,
	}
	go result.run()
	return result
}

func (f *delayedFunc) run() {
	<-f.timer.C
	if atomic.LoadInt32(&f.canceled) != 0 {
		return
	}
	f.fn()
}

// Cancel the execution of a delayed func, if it has not been executed yet.
func (f *delayedFunc) Cancel() {
	atomic.StoreInt32(&f.canceled, 1)
	f.timer.Stop()
}
