package table

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	ydb "github.com/doublecloud/tross/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/testutil"
)

func TestRetryerBackoffRetryCancelation(t *testing.T) {
	for _, testErr := range []error{
		// Errors leading to backoff repeat.
		&ydb.TransportError{
			Reason: ydb.TransportErrorResourceExhausted,
		},
		fmt.Errorf("wrap transport error: %w", &ydb.TransportError{
			Reason: ydb.TransportErrorResourceExhausted,
		}),
		&ydb.OpError{
			Reason: ydb.StatusOverloaded,
		},
		fmt.Errorf("wrap op error: %w", &ydb.OpError{
			Reason: ydb.StatusOverloaded,
		}),
	} {
		t.Run("", func(t *testing.T) {
			backoff := make(chan chan time.Time)
			bacoffFunc := ydb.BackoffFunc(func(n int) <-chan time.Time {
				ch := make(chan time.Time)
				backoff <- ch
				return ch
			})
			r := Retryer{
				MaxRetries:      1,
				FastBackoff:     bacoffFunc,
				SlowBackoff:     bacoffFunc,
				SessionProvider: SingleSession(simpleSession()),
			}

			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan error)
			go func() {
				result <- r.Do(ctx, OperationFunc(func(ctx context.Context, _ *Session) error {
					return testErr
				}))
			}()

			select {
			case <-backoff:
			case err := <-result:
				t.Fatalf("unexpected result: %v", err)
			}

			cancel()
			if err := <-result; !errors.Is(err, testErr) {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestRetryerImmediateRetry(t *testing.T) {
	for testErr, session := range map[error]*Session{
		&ydb.TransportError{
			Reason: ydb.TransportErrorResourceExhausted,
		}: newSession(t, 1, nil),
		&ydb.TransportError{
			Reason: ydb.TransportErrorAborted,
		}: newSession(t, 2, nil),
		&ydb.OpError{
			Reason: ydb.StatusUnavailable,
		}: newSession(t, 3, nil),
		&ydb.OpError{
			Reason: ydb.StatusOverloaded,
		}: newSession(t, 4, nil),
		&ydb.OpError{
			Reason: ydb.StatusAborted,
		}: newSession(t, 5, nil),
		&ydb.OpError{
			Reason: ydb.StatusNotFound,
		}: newSession(t, 6, nil),
		fmt.Errorf("wrap op error: %w", &ydb.OpError{
			Reason: ydb.StatusAborted,
		}): newSession(t, 7, nil),
	} {
		t.Run(fmt.Sprintf("err: %v, session: %v", testErr, session != nil), func(t *testing.T) {
			var count int
			r := Retryer{
				MaxRetries:   3,
				RetryChecker: ydb.DefaultRetryChecker,
				SessionProvider: SessionProviderFunc{
					OnGet: func(ctx context.Context) (s *Session, err error) {
						if session != nil {
							return session, nil
						}
						return nil, testErr
					},
				},
			}
			err := r.Do(
				context.Background(),
				OperationFunc(func(ctx context.Context, _ *Session) error {
					count++
					return testErr
				}),
			)
			if act, exp := count, r.MaxRetries+1; act != exp {
				t.Errorf("unexpected operation calls: %v; want %v", act, exp)
			}
			if !errors.Is(err, testErr) {
				t.Fatalf("unexpected error: %v; want: %v", err, testErr)
			}
		})
	}
}

func TestRetryerBadSession(t *testing.T) {
	client := &Client{
		Driver: &testutil.Driver{
			OnCall: func(ctx context.Context, m testutil.MethodCode, req, res interface{}) error {
				return nil
			},
		},
	}
	r := Retryer{
		MaxRetries: 3,
		SessionProvider: SessionProviderFunc{
			OnGet: client.CreateSession,
		},
	}

	var sessions []*Session
	err := r.Do(
		context.Background(),
		OperationFunc(func(ctx context.Context, s *Session) error {
			sessions = append(sessions, s)
			return &ydb.OpError{
				Reason: ydb.StatusBadSession,
			}
		}),
	)
	if !ydb.IsOpError(err, ydb.StatusBadSession) {
		t.Errorf("unexpected error: %v", err)
	}
	if act, exp := len(sessions), r.MaxRetries+1; act != exp {
		t.Errorf("unexpected operation calls: %v; want %v", act, exp)
	}
	seen := make(map[*Session]bool, len(sessions))
	for _, s := range sessions {
		if seen[s] {
			t.Errorf("session used twice")
		} else {
			seen[s] = true
		}
		s.closeMux.Lock()
		if !s.closed {
			t.Errorf("bad session was not closed")
		}
		s.closeMux.Unlock()
	}
}

func TestRetryerBadSessionReuse(t *testing.T) {
	client := &Client{
		Driver: &testutil.Driver{
			OnCall: func(ctx context.Context, m testutil.MethodCode, req, res interface{}) error {
				return nil
			},
		},
	}
	var (
		sessions = make([]*Session, 10)
		bad      = make(map[*Session]bool)
		reused   = make(map[*Session]bool)
	)
	for i := range sessions {
		s, err := client.CreateSession(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		sessions[i] = s
		bad[s] = i < len(sessions)-1 // All bad but last.
	}
	var i int
	r := Retryer{
		MaxRetries: len(sessions),
		SessionProvider: SessionProviderFunc{
			OnGet: func(_ context.Context) (*Session, error) {
				defer func() { i++ }()
				return sessions[i], nil
			},
			OnPut: func(_ context.Context, s *Session) error {
				reused[s] = true
				return nil
			},
		},
	}
	_ = r.Do(
		context.Background(),
		OperationFunc(func(ctx context.Context, s *Session) error {
			if bad[s] {
				return &ydb.OpError{
					Reason: ydb.StatusBadSession,
				}
			}
			return nil
		}),
	)
	for _, s := range sessions {
		if bad[s] && reused[s] {
			t.Errorf("reused bad session")
		}
		if !bad[s] && !reused[s] {
			t.Errorf("missed good session")
		}
	}
}

func TestRetryerImmediateReturn(t *testing.T) {
	for _, testErr := range []error{
		&ydb.OpError{
			Reason: ydb.StatusGenericError,
		},
		fmt.Errorf("wrap op error: %w", &ydb.OpError{
			Reason: ydb.StatusGenericError,
		}),
		&ydb.TransportError{
			Reason: ydb.TransportErrorPermissionDenied,
		},
		fmt.Errorf("wrap transport error: %w", &ydb.TransportError{
			Reason: ydb.TransportErrorPermissionDenied,
		}),
		errors.New("whoa"),
	} {
		t.Run("", func(t *testing.T) {
			var count int32
			r := Retryer{
				MaxRetries:      1e6,
				RetryChecker:    ydb.DefaultRetryChecker,
				SessionProvider: SingleSession(simpleSession()),
			}
			err := r.Do(
				context.Background(),
				OperationFunc(func(ctx context.Context, _ *Session) error {
					if !atomic.CompareAndSwapInt32(&count, 0, 1) {
						t.Fatalf("unexpected repeat")
					}
					return testErr
				}),
			)
			if !errors.Is(err, testErr) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// We are testing all suspentions of custom operation func against to all context
// timeouts - all sub-tests must have latency less than timeouts (+tolerance)
func TestRetryContextDeadline(t *testing.T) {
	tolerance := 10 * time.Millisecond
	timeouts := []time.Duration{
		50 * time.Millisecond,
		100 * time.Millisecond,
		200 * time.Millisecond,
		500 * time.Millisecond,
		time.Second,
	}
	sleeps := []time.Duration{
		time.Nanosecond,
		time.Microsecond,
		time.Millisecond,
		10 * time.Millisecond,
		50 * time.Millisecond,
		100 * time.Millisecond,
		500 * time.Millisecond,
		time.Second,
		5 * time.Second,
	}
	errs := []error{
		io.EOF,
		context.DeadlineExceeded,
		fmt.Errorf("test error"),
		&ydb.TransportError{
			Reason: ydb.TransportErrorUnknownCode,
		},
		&ydb.TransportError{
			Reason: ydb.TransportErrorCanceled,
		},
		&ydb.TransportError{
			Reason: ydb.TransportErrorUnknown,
		},
		&ydb.TransportError{
			Reason: ydb.TransportErrorInvalidArgument,
		},
		&ydb.TransportError{
			Reason: ydb.TransportErrorDeadlineExceeded,
		},
		&ydb.TransportError{
			Reason: ydb.TransportErrorNotFound,
		},
		&ydb.TransportError{
			Reason: ydb.TransportErrorAlreadyExists,
		},
		&ydb.TransportError{
			Reason: ydb.TransportErrorPermissionDenied,
		},
		&ydb.TransportError{
			Reason: ydb.TransportErrorResourceExhausted,
		},
		&ydb.TransportError{
			Reason: ydb.TransportErrorFailedPrecondition,
		},
		&ydb.TransportError{
			Reason: ydb.TransportErrorAborted,
		},
		&ydb.TransportError{
			Reason: ydb.TransportErrorOutOfRange,
		},
		&ydb.TransportError{
			Reason: ydb.TransportErrorUnimplemented,
		},
		&ydb.TransportError{
			Reason: ydb.TransportErrorInternal,
		},
		&ydb.TransportError{
			Reason: ydb.TransportErrorUnavailable,
		},
		&ydb.TransportError{
			Reason: ydb.TransportErrorDataLoss,
		},
		&ydb.TransportError{
			Reason: ydb.TransportErrorUnauthenticated,
		},
		&ydb.OpError{
			Reason: ydb.StatusUnknownStatus,
		},
		&ydb.OpError{
			Reason: ydb.StatusBadRequest,
		},
		&ydb.OpError{
			Reason: ydb.StatusUnauthorized,
		},
		&ydb.OpError{
			Reason: ydb.StatusInternalError,
		},
		&ydb.OpError{
			Reason: ydb.StatusAborted,
		},
		&ydb.OpError{
			Reason: ydb.StatusUnavailable,
		},
		&ydb.OpError{
			Reason: ydb.StatusOverloaded,
		},
		&ydb.OpError{
			Reason: ydb.StatusSchemeError,
		},
		&ydb.OpError{
			Reason: ydb.StatusGenericError,
		},
		&ydb.OpError{
			Reason: ydb.StatusTimeout,
		},
		&ydb.OpError{
			Reason: ydb.StatusBadSession,
		},
		&ydb.OpError{
			Reason: ydb.StatusPreconditionFailed,
		},
		&ydb.OpError{
			Reason: ydb.StatusAlreadyExists,
		},
		&ydb.OpError{
			Reason: ydb.StatusNotFound,
		},
		&ydb.OpError{
			Reason: ydb.StatusSessionExpired,
		},
		&ydb.OpError{
			Reason: ydb.StatusCancelled,
		},
		&ydb.OpError{
			Reason: ydb.StatusUndetermined,
		},
		&ydb.OpError{
			Reason: ydb.StatusUnsupported,
		},
		&ydb.OpError{
			Reason: ydb.StatusSessionBusy,
		},
	}
	client := &Client{
		Driver: &testutil.Driver{
			OnCall: func(ctx context.Context, m testutil.MethodCode, req, res interface{}) error {
				return nil
			},
		},
	}
	r := Retryer{
		MaxRetries:   1e6,
		RetryChecker: ydb.DefaultRetryChecker,
		SessionProvider: SessionProviderFunc{
			OnGet: client.CreateSession,
		},
	}
	for i := range timeouts {
		for j := range sleeps {
			timeout := timeouts[i]
			sleep := sleeps[j]
			t.Run(fmt.Sprintf("timeout %v, sleep %v", timeout, sleep), func(t *testing.T) {
				random := rand.New(rand.NewSource(time.Now().Unix()))
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()
				_ = r.Do(
					WithRetryTrace(
						ctx,
						RetryTrace{
							OnLoop: func(info RetryLoopStartInfo) func(RetryLoopDoneInfo) {
								return func(info RetryLoopDoneInfo) {
									if info.Latency-timeouts[i] > tolerance {
										t.Errorf("unexpected latency: %v (attempts %d)", info.Latency, info.Attempts)
									}
								}
							},
						},
					),
					OperationFunc(func(ctx context.Context, _ *Session) error {
						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-time.After(sleep):
							return errs[random.Intn(len(errs))]
						}
					}),
				)
			})
		}
	}
}
