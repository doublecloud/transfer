package ydb

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"google.golang.org/grpc"
)

func TestLogBackoff(t *testing.T) {
	type exp struct {
		eq  time.Duration
		gte time.Duration
		lte time.Duration
	}
	for _, test := range []struct {
		name    string
		backoff LogBackoff
		exp     []exp
		seeds   int64
	}{
		{
			backoff: LogBackoff{
				SlotDuration: time.Second,
				Ceiling:      3,
				JitterLimit:  0,
			},
			exp: []exp{
				{gte: 0, lte: time.Second},     // 1 << min(0, 3)
				{gte: 0, lte: 2 * time.Second}, // 1 << min(1, 3)
				{gte: 0, lte: 4 * time.Second}, // 1 << min(2, 3)
				{gte: 0, lte: 8 * time.Second}, // 1 << min(3, 3)
				{gte: 0, lte: 8 * time.Second}, // 1 << min(4, 3)
				{gte: 0, lte: 8 * time.Second}, // 1 << min(5, 3)
				{gte: 0, lte: 8 * time.Second}, // 1 << min(6, 3)
			},
			seeds: 1000,
		},
		{
			backoff: LogBackoff{
				SlotDuration: time.Second,
				Ceiling:      3,
				JitterLimit:  0.5,
			},
			exp: []exp{
				{gte: 500 * time.Millisecond, lte: time.Second}, // 1 << min(0, 3)
				{gte: 1 * time.Second, lte: 2 * time.Second},    // 1 << min(1, 3)
				{gte: 2 * time.Second, lte: 4 * time.Second},    // 1 << min(2, 3)
				{gte: 4 * time.Second, lte: 8 * time.Second},    // 1 << min(3, 3)
				{gte: 4 * time.Second, lte: 8 * time.Second},    // 1 << min(4, 3)
				{gte: 4 * time.Second, lte: 8 * time.Second},    // 1 << min(5, 3)
				{gte: 4 * time.Second, lte: 8 * time.Second},    // 1 << min(6, 3)
			},
			seeds: 1000,
		},
		{
			backoff: LogBackoff{
				SlotDuration: time.Second,
				Ceiling:      3,
				JitterLimit:  1,
			},
			exp: []exp{
				{eq: time.Second},     // 1 << min(0, 3)
				{eq: 2 * time.Second}, // 1 << min(1, 3)
				{eq: 4 * time.Second}, // 1 << min(2, 3)
				{eq: 8 * time.Second}, // 1 << min(3, 3)
				{eq: 8 * time.Second}, // 1 << min(4, 3)
				{eq: 8 * time.Second}, // 1 << min(5, 3)
				{eq: 8 * time.Second}, // 1 << min(6, 3)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.seeds == 0 {
				test.seeds = 1
			}
			for seed := int64(0); seed < test.seeds; seed++ {
				// Fix random to reproduce the tests.
				rand.Seed(seed)

				for n, exp := range test.exp {
					act := test.backoff.Delay(n)
					if exp := exp.eq; exp != 0 {
						if exp != act {
							t.Fatalf(
								"unexpected backoff delay: %s; want %s",
								act, exp,
							)
						}
						continue
					}
					if gte := exp.gte; act < gte {
						t.Errorf(
							"unexpected backoff delay: %s; want >= %s",
							act, gte,
						)
					}
					if lte := exp.lte; act > lte {
						t.Errorf(
							"unexpected backoff delay: %s; want <= %s",
							act, lte,
						)
					}
				}
			}
		})
	}
}

func TestRetryModes(t *testing.T) {
	type CanRetry struct {
		idempotentOperation    bool // after an error we must retry idempotent operation or no
		nonIdempotentOperation bool // after an error we must retry non-idempotent operation or no
	}
	type Case struct {
		err           error       // given error
		backoff       BackoffType // type of backoff: no backoff (=== no operationCompleted), fast backoff, slow backoff
		deleteSession bool        // close session and delete from pool
		canRetry      CanRetry
	}
	errs := []Case{
		{
			err:           fmt.Errorf("unknown error"), // retryer given unknown error - we will not operationCompleted and will close session
			backoff:       BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err:           context.DeadlineExceeded, // golang context deadline exceeded
			backoff:       BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err:           context.Canceled, // golang context cancelled
			backoff:       BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err:           mapGRPCError(grpc.ErrClientConnClosing),
			backoff:       BackoffTypeFastBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &TransportError{
				Reason: TransportErrorUnknownCode,
			},
			backoff:       BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &TransportError{
				Reason: TransportErrorCanceled,
			},
			backoff:       BackoffTypeFastBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &TransportError{
				Reason: TransportErrorUnknown,
			},
			backoff:       BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &TransportError{
				Reason: TransportErrorInvalidArgument,
			},
			backoff:       BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &TransportError{
				Reason: TransportErrorDeadlineExceeded,
			},
			backoff:       BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &TransportError{
				Reason: TransportErrorNotFound,
			},
			backoff:       BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &TransportError{
				Reason: TransportErrorAlreadyExists,
			},
			backoff:       BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &TransportError{
				Reason: TransportErrorPermissionDenied,
			},
			backoff:       BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &TransportError{
				Reason: TransportErrorResourceExhausted,
			},
			backoff:       BackoffTypeSlowBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: true,
			},
		},
		{
			err: &TransportError{
				Reason: TransportErrorFailedPrecondition,
			},
			backoff:       BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &TransportError{
				Reason: TransportErrorAborted,
			},
			backoff:       BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: true,
			},
		},
		{
			err: &TransportError{
				Reason: TransportErrorOutOfRange,
			},
			backoff:       BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &TransportError{
				Reason: TransportErrorUnimplemented,
			},
			backoff:       BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &TransportError{
				Reason: TransportErrorInternal,
			},
			backoff:       BackoffTypeFastBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &TransportError{
				Reason: TransportErrorUnavailable,
			},
			backoff:       BackoffTypeFastBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &TransportError{
				Reason: TransportErrorDataLoss,
			},
			backoff:       BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &TransportError{
				Reason: TransportErrorUnauthenticated,
			},
			backoff:       BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &OpError{
				Reason: StatusUnknownStatus,
			},
			backoff:       BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &OpError{
				Reason: StatusBadRequest,
			},
			backoff:       BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &OpError{
				Reason: StatusUnauthorized,
			},
			backoff:       BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &OpError{
				Reason: StatusInternalError,
			},
			backoff:       BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &OpError{
				Reason: StatusAborted,
			},
			backoff:       BackoffTypeFastBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: true,
			},
		},
		{
			err: &OpError{
				Reason: StatusUnavailable,
			},
			backoff:       BackoffTypeFastBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: true,
			},
		},
		{
			err: &OpError{
				Reason: StatusOverloaded,
			},
			backoff:       BackoffTypeSlowBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: true,
			},
		},
		{
			err: &OpError{
				Reason: StatusSchemeError,
			},
			backoff:       BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &OpError{
				Reason: StatusGenericError,
			},
			backoff:       BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &OpError{
				Reason: StatusTimeout,
			},
			backoff:       BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &OpError{
				Reason: StatusBadSession,
			},
			backoff:       BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: true,
			},
		},
		{
			err: &OpError{
				Reason: StatusPreconditionFailed,
			},
			backoff:       BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &OpError{
				Reason: StatusAlreadyExists,
			},
			backoff:       BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &OpError{
				Reason: StatusNotFound,
			},
			backoff:       BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: true,
			},
		},
		{
			err: &OpError{
				Reason: StatusSessionExpired,
			},
			backoff:       BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &OpError{
				Reason: StatusCancelled,
			},
			backoff:       BackoffTypeFastBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &OpError{
				Reason: StatusUndetermined,
			},
			backoff:       BackoffTypeFastBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &OpError{
				Reason: StatusUnsupported,
			},
			backoff:       BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &OpError{
				Reason: StatusSessionBusy,
			},
			backoff:       BackoffTypeFastBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: true,
			},
		},
	}
	r := DefaultRetryChecker
	for _, test := range errs {
		t.Run(test.err.Error(), func(t *testing.T) {
			m := r.Check(test.err)
			if m.MustRetry(true) != test.canRetry.idempotentOperation {
				t.Errorf("unexpected must retry idempotent operation status: %v, want: %v", m.MustRetry(true), test.canRetry.idempotentOperation)
			}
			if m.MustRetry(false) != test.canRetry.nonIdempotentOperation {
				t.Errorf("unexpected must retry non-idempotent operation status: %v, want: %v", m.MustRetry(false), test.canRetry.nonIdempotentOperation)
			}
			if m.backoff != test.backoff {
				t.Errorf("unexpected backoff status: %v, want: %v", m.backoff, test.backoff)
			}
			if m.deleteSession != test.deleteSession {
				t.Errorf("unexpected delete session status: %v, want: %v", m.deleteSession, test.deleteSession)
			}
		})
	}
}
