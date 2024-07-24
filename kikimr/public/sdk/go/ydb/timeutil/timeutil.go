package timeutil

import (
	"math"
	"time"
)

// Now returns result of time.Now() if no TestHookTimeNow set up.
func Now() time.Time {
	if f := testHookTimeNow; f != nil {
		return f()
	}
	return time.Now()
}

// Until returns the duration until t.
// It is shorthand for t.Sub(timeutil.Now()).
func Until(t time.Time) time.Duration {
	return t.Sub(Now())
}

// Timer is the interface used by node watcher to be periodically triggered to
// prepare some action.
type Timer interface {
	Reset(time.Duration) bool
	Stop() bool
	C() <-chan time.Time
}

// NewTimer creates a new Timer that will send the current time on its channel
// after at least duration d.
//
// It uses time package as timer implementation.
func NewTimer(d time.Duration) Timer {
	if f := testHookNewTimer; f != nil {
		return f(d)
	}
	return timeTimer{time.NewTimer(d)}
}

// NewStoppedTimer creates a new stopped Timer.
func NewStoppedTimer() Timer {
	t := NewTimer(time.Duration(math.MaxInt64))
	if !t.Stop() {
		panic("ydb: timeutil: can not created stopped timer")
	}
	return t
}

// AfterFunc waits for the duration to elapse and then calls f
// in its own goroutine. It returns a Timer that can
// be used to cancel the call using its Stop method.
func AfterFunc(d time.Duration, f func()) Timer {
	return timeTimer{time.AfterFunc(d, f)}
}

type timeTimer struct {
	t *time.Timer
}

func (t timeTimer) C() <-chan time.Time {
	return t.t.C
}

func (t timeTimer) Reset(d time.Duration) bool {
	return t.t.Reset(d)
}

func (t timeTimer) Stop() bool {
	return t.t.Stop()
}
