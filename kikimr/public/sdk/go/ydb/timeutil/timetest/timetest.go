package timetest

import (
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/timeutil"
)

type SingleTimerStub struct {
	TimerStub
	Cleanup func()
}

type TimerStub struct {
	C       chan time.Time
	Created chan time.Duration
	Reset   chan time.Duration

	once sync.Once
}

func (t *TimerStub) Init() (success bool) {
	t.once.Do(func() {
		success = true
	})
	return
}

func stack(skip, buf int) (names []string) {
	// No need to track this function and runtime.Callers frame, thus `+2`
	// is below.
	p := make([]uintptr, buf)
	n := runtime.Callers(2+skip, p)
	if n == 0 {
		panic("timetest: no caller frame")
	}
	frames := runtime.CallersFrames(p[:n])
	for {
		frame, more := frames.Next()
		path := strings.Split(frame.Function, ".")
		name := path[len(path)-1]
		names = append(names, name)
		if !more {
			break
		}
	}
	return
}

type TimerStubs map[string]*TimerStub

func StubTimers(t *testing.T, callers ...string) (ts TimerStubs, cleanup func()) {
	ts = make(TimerStubs, len(callers))
	for _, c := range callers {
		ts[c] = &TimerStub{
			C:       make(chan time.Time),
			Created: make(chan time.Duration, 1),
			Reset:   make(chan time.Duration),
		}
	}
	cleanup = timeutil.StubTestHookNewTimer(func(d time.Duration) (r timeutil.Timer) {
		var (
			s     *TimerStub
			chain string
		)
		for _, f := range stack(2, 2) {
			if chain != "" {
				chain += " <- "
			}
			chain += f
			if s = ts[f]; s != nil {
				break
			}
		}
		if s == nil {
			t.Fatalf("unexpected timeutil.NewTimer() from %s", chain)
			return nil
		}
		if !s.Init() {
			t.Fatalf("timeutil.NewTimer() is called more than once from %s", chain)
			return nil
		}
		s.Created <- d
		return Timer{
			Ch: s.C,
			OnReset: func(d time.Duration) bool {
				s.Reset <- d
				return true
			},
		}
	})
	return
}

func StubSingleTimer(t *testing.T) *SingleTimerStub {
	s := &SingleTimerStub{
		TimerStub: TimerStub{
			C:       make(chan time.Time),
			Created: make(chan time.Duration, 1),
			Reset:   make(chan time.Duration),
		},
	}
	s.Cleanup = timeutil.StubTestHookNewTimer(func(d time.Duration) (r timeutil.Timer) {
		if !s.Init() {
			t.Fatalf("timeutil.NewTimer() is called more than once")
			return nil
		}
		s.Created <- d
		return Timer{
			Ch: s.C,
			OnReset: func(d time.Duration) bool {
				s.Reset <- d
				return true
			},
		}
	})
	return s
}

type Timer struct {
	Ch      <-chan time.Time
	OnReset func(time.Duration) bool
	OnStop  func() bool
}

func (t Timer) Reset(d time.Duration) bool {
	if t.OnReset != nil {
		return t.OnReset(d)
	}
	return true
}

func (t Timer) Stop() bool {
	if t.OnStop != nil {
		return t.OnStop()
	}
	return true
}

func (t Timer) C() <-chan time.Time {
	return t.Ch
}
