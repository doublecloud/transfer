package timeutil

import (
	"sync"
	"time"
)

var (
	// If testHookTimeNow is non-nil, then it overrides time.Now() calls
	// within this package.
	testHookTimeNow func() time.Time

	// If testHookNewTimer is non-nil, then it overrides NewTimer()
	// calls within this package.
	testHookNewTimer func(time.Duration) Timer
)

// StubTestHookTimeNow stubs all `Now()` use for ydb packages.
//
// It sets up current time to given now time.
// It returns time shifter function that shifts current time by given
// duration.
// It returns cleanup function that MUST be called after test execution.
//
// NOTE: tests using this function MUST not be called concurrently.
func StubTestHookTimeNow(now time.Time) (shifter func(time.Duration), cleanup func()) {
	orig := testHookTimeNow
	cleanup = func() {
		testHookTimeNow = orig
	}
	var mu sync.Mutex
	testHookTimeNow = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	shifter = func(d time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		now = now.Add(d)
	}
	return
}

// StubTestHookNewTimer stubs all creation of Timers for ydb packages.
// It returns cleanup function that MUST be called after test execution.
//
// NOTE: tests using this function MUST not be called concurrently.
func StubTestHookNewTimer(f func(time.Duration) Timer) (cleanup func()) {
	orig := testHookNewTimer
	cleanup = func() {
		testHookNewTimer = orig
	}
	testHookNewTimer = f
	return
}
