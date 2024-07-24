package ydb

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/timeutil"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/timeutil/timetest"
)

func TestRepeater(t *testing.T) {
	timerC := make(chan time.Time, 1)
	timer := timetest.Timer{
		Ch: timerC,
	}
	cleanup := timeutil.StubTestHookNewTimer(func(time.Duration) timeutil.Timer {
		return timer
	})
	defer cleanup()

	exec := make(chan struct{}, 1)
	r := NewRepeater(42*time.Second, 0,
		func(_ context.Context) {
			exec <- struct{}{}
		})

	timerC <- time.Now()
	assertRecv(t, 500*time.Millisecond, exec)
	assertNoRecv(t, 50*time.Millisecond, exec)

	r.Stop()
	timerC <- time.Now()
	assertNoRecv(t, 50*time.Millisecond, exec)
}

func TestRepeaterCancelation(t *testing.T) {
	var (
		timerC = make(chan time.Time)
		enter  = make(chan struct{}, 2)
		exit   = make(chan struct{}, 2)
	)
	timer := timetest.Timer{
		Ch: timerC,
	}
	cleanup := timeutil.StubTestHookNewTimer(func(time.Duration) timeutil.Timer {
		return timer
	})
	defer cleanup()

	r := NewRepeater(42*time.Second, 0,
		func(ctx context.Context) {
			enter <- struct{}{}
			<-ctx.Done()
			exit <- struct{}{}
		})

	// Run callback in a separate goroutine to avoid deadlock.
	// That is, StubTimer run its function in the same goroutine as Emit
	// called.
	go func() { timerC <- time.Now() }()

	const timeout = 100 * time.Millisecond

	assertRecv(t, timeout, enter)
	assertNoRecv(t, timeout, enter)
	assertNoRecv(t, timeout, exit)

	r.Stop()

	assertRecv(t, timeout, exit)
}

func recv(ch interface{}, timeout time.Duration) error {
	i, _, _ := reflect.Select([]reflect.SelectCase{
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)},
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(time.After(timeout))},
	})
	if i == 0 {
		return nil
	}
	return fmt.Errorf("timed out: %s", timeout)
}

func noRecv(ch interface{}, timeout time.Duration) error {
	i, _, _ := reflect.Select([]reflect.SelectCase{
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)},
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(time.After(timeout))},
	})
	if i == 1 {
		return nil
	}
	return fmt.Errorf("unexepcted filling")
}

func assertRecv(t *testing.T, timeout time.Duration, ch interface{}) {
	if err := recv(ch, timeout); err != nil {
		t.Fatalf("%s: %v", fileLine(2), err)
	}
}

func assertNoRecv(t *testing.T, timeout time.Duration, ch interface{}) {
	if err := noRecv(ch, timeout); err != nil {
		t.Fatalf("%s: %v", fileLine(2), err)
	}
}

func fileLine(skip int) string {
	_, file, line, _ := runtime.Caller(skip)
	return filepath.Base(file) + ":" + strconv.Itoa(line)
}
