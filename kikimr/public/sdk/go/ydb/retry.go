package ydb

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"
)

// Default parameters used by Retry() functions within different sub packages.
const (
	DefaultFastSlot = 5 * time.Millisecond
	DefaultSlowSlot = 1 * time.Second
)

// Default parameters used by Retry() functions within different sub packages.
var (
	// Deprecated: will be redeclared as constant at next major release,
	// use as constant instead and configure max retries as parameter of Retryer
	DefaultMaxRetries   = 10
	DefaultRetryChecker = RetryChecker{}
	// DefaultBackoff is a logarithmic backoff operationCompleted strategy.
	// Deprecated: use DefaultFastBackoff or DefaultSlowBackoff instead
	DefaultBackoff = LogBackoff{
		SlotDuration: time.Second,
		Ceiling:      6,
	}
	DefaultFastBackoff = LogBackoff{
		SlotDuration: DefaultFastSlot,
		Ceiling:      6,
	}
	DefaultSlowBackoff = LogBackoff{
		SlotDuration: DefaultSlowSlot,
		Ceiling:      6,
	}
)

// RetryChecker contains options of checking errors returned by YDB for ability
// to operationCompleted provoked operation.
type RetryChecker struct {
	// RetryNotFound reports whether Repeater must operationCompleted ErrNotFound errors.
	// Deprecated: has no effect now
	RetryNotFound bool
}

// RetryMode reports whether operation is able to be retried and with which
// properties.
type RetryMode struct {
	operationCompleted OperationCompleted
	backoff            BackoffType
	deleteSession      bool
}

// Deprecated: has no effect now
const (
	RetryUnavailable = 1 << iota >> 1
	RetryAvailable
	RetryBackoff
	RetryDeleteSession
	RetryCheckSession
	RetryDropCache
)

// BackoffType reports how to backoff operation
type BackoffType uint8

// Binary flags that used as BackoffType
const (
	BackoffTypeNoBackoff BackoffType = 1 << iota >> 1

	BackoffTypeFastBackoff
	BackoffTypeSlowBackoff

	backoffTypeBackoffAny = BackoffTypeFastBackoff | BackoffTypeSlowBackoff
)

func (b BackoffType) String() string {
	switch b {
	case BackoffTypeNoBackoff:
		return "immediately"
	case BackoffTypeFastBackoff:
		return "fast backoff"
	case BackoffTypeSlowBackoff:
		return "slow backoff"
	case backoffTypeBackoffAny:
		return "any backoff"
	default:
		return fmt.Sprintf("unknown backoff type %d", b)
	}
}

// OperationCompleted reports which operations need to operationCompleted
type OperationCompleted uint8

// Binary flags that used as OperationCompleted
const (
	OperationCompletedTrue      OperationCompleted = 1 << iota >> 1
	OperationCompletedUndefined                    // may be true or may be false
	OperationCompletedFalse
)

func (t OperationCompleted) String() string {
	switch t {
	case OperationCompletedTrue:
		return "operation was completed"
	case OperationCompletedFalse:
		return "operation was not completed"
	case OperationCompletedUndefined:
		return "operation completed status undefined"
	default:
		return fmt.Sprintf("unknown operation completed code: %d", t)
	}
}

// Deprecated: will be dropped at next major release
func (m RetryMode) Retriable() bool { return m.operationCompleted&OperationCompletedUndefined != 0 }

// Deprecated: will be dropped at next major release
func (m RetryMode) MustCheckSession() bool { return m.deleteSession }

// Deprecated: will be dropped at next major release
func (m RetryMode) MustDropCache() bool { return m.deleteSession }

func (m RetryMode) MustRetry(isOperationIdempotent bool) bool {
	switch m.operationCompleted {
	case OperationCompletedTrue:
		return false
	case OperationCompletedUndefined:
		return isOperationIdempotent
	default:
		return true
	}
}

func (m RetryMode) MustBackoff() bool        { return m.backoff&backoffTypeBackoffAny != 0 }
func (m RetryMode) BackoffType() BackoffType { return m.backoff }

func (m RetryMode) MustDeleteSession() bool { return m.deleteSession }

// Check returns operationCompleted mode for err.
func (r *RetryChecker) Check(err error) (m RetryMode) {
	var te *TransportError
	var oe *OpError

	switch {
	case errors.As(err, &te):
		return RetryMode{
			operationCompleted: te.Reason.operationCompleted(),
			backoff:            te.Reason.backoffType(),
			deleteSession:      te.Reason.mustDeleteSession(),
		}
	case errors.As(err, &oe):
		return RetryMode{
			operationCompleted: oe.Reason.operationCompleted(),
			backoff:            oe.Reason.backoffType(),
			deleteSession:      oe.Reason.mustDeleteSession(),
		}
	default:
		return RetryMode{
			operationCompleted: OperationCompletedTrue,
			backoff:            BackoffTypeNoBackoff,
			deleteSession:      false,
		}
	}
}

// Backoff is the interface that contains logic of delaying operation operationCompleted.
type Backoff interface {
	// Wait maps index of the operationCompleted to a channel which fulfillment means that
	// delay is over.
	//
	// Note that operationCompleted index begins from 0 and 0-th index means that it is the
	// first operationCompleted attempt after an initial error.
	Wait(n int) <-chan time.Time
}

// BackoffFunc is an adatper to allow the use of ordinary functions as Backoff.
type BackoffFunc func(n int) <-chan time.Time

// Wait implements Backoff interface.
func (f BackoffFunc) Wait(n int) <-chan time.Time {
	return f(n)
}

// WaitBackoff is a helper function that waits for i-th backoff b or ctx
// expiration.
// It returns non-nil error if and only if context expiration branch wins.
func WaitBackoff(ctx context.Context, b Backoff, i int) error {
	if b == nil {
		return ctx.Err()
	}
	select {
	case <-b.Wait(i):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// LogBackoff contains logarithmic backoff policy.
type LogBackoff struct {
	// SlotDuration is a size of a single time slot used in backoff delay
	// calculation.
	// If SlotDuration is less or equal to zero, then the time.Second value is
	// used.
	SlotDuration time.Duration

	// Ceiling is a maximum degree of backoff delay growth.
	// If Ceiling is less or equal to zero, then the default ceiling of 1 is
	// used.
	Ceiling uint

	// JitterLimit controls fixed and random portions of backoff delay.
	// Its value can be in range [0, 1].
	// If JitterLimit is non zero, then the backoff delay will be equal to (F + R),
	// where F is a result of multiplication of this value and calculated delay
	// duration D; and R is a random sized part from [0,(D - F)].
	JitterLimit float64
}

// Wait implements Backoff interface.
func (b LogBackoff) Wait(n int) <-chan time.Time {
	return time.After(b.Delay(n))
}

// Delay returns mapping of i to delay.
func (b LogBackoff) Delay(i int) time.Duration {
	s := b.SlotDuration
	if s <= 0 {
		s = time.Second
	}
	n := 1 << min(uint(i), max(1, b.Ceiling))
	d := s * time.Duration(n)
	f := time.Duration(math.Min(1, math.Abs(b.JitterLimit)) * float64(d))
	if f == d {
		return f
	}
	return f + time.Duration(rand.Intn(int(d-f)+1))
}

func min(a, b uint) uint {
	if a < b {
		return a
	}
	return b
}

func max(a, b uint) uint {
	if a > b {
		return a
	}
	return b
}
