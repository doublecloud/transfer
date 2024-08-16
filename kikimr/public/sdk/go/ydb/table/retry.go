package table

import (
	"context"
	"errors"
	"time"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb"
)

// SessionProvider is the interface that holds session lifecycle logic.
type SessionProvider interface {
	// Get returns alive idle session or creates new one.
	Get(context.Context) (*Session, error)

	// Put takes no longer needed session for reuse or deletion depending
	// on implementation.
	// Put must be fast, if necessary must be async
	Put(context.Context, *Session) (err error)

	// PutBusy takes session with not yet completed operation inside.
	// It gives full ownership of s to session provider.
	// PutBusy must be fast, if necessary must be async
	// Deprecated: use conditionally Put() or CloseSession() for uncertain
	// sessions instead
	PutBusy(context.Context, *Session) (err error)

	// CloseSession provides the most effective way of session closing
	// instead of plain session.Close.
	// CloseSession must be fast. If necessary, can be async.
	CloseSession(ctx context.Context, s *Session) error
}

type SessionProviderFunc struct {
	OnGet func(context.Context) (*Session, error)
	OnPut func(context.Context, *Session) error
	// Deprecated: has no effect now
	OnPutBusy func(context.Context, *Session) error
}

func (f SessionProviderFunc) Get(ctx context.Context) (*Session, error) {
	if f.OnGet == nil {
		return nil, errNoSession
	}
	return f.OnGet(ctx)
}

func (f SessionProviderFunc) Put(ctx context.Context, s *Session) error {
	if f.OnPut == nil {
		return errSessionOverflow
	}
	return f.OnPut(ctx, s)
}

// Deprecated: wull be dropped at next major release
func (f SessionProviderFunc) PutBusy(ctx context.Context, s *Session) error {
	return s.Close(ctx)
}

func (f SessionProviderFunc) CloseSession(ctx context.Context, s *Session) error {
	return s.Close(ctx)
}

// SingleSession returns SessionProvider that uses only given session durting
// retries.
func SingleSession(s *Session) SessionProvider {
	return &singleSession{s: s}
}

// Operation is the interface that holds an operation for retry.
type Operation interface {
	// Do prepares actions need to be done for this operation.
	//
	// Implementations MUST NOT wrap ydb/table errors in order to leave the
	// ability to distinguish error type and make a decision about the next
	// retry attempt.
	Do(context.Context, *Session) error
}

// OperationFunc is an adapter to allow the use of ordinary functions as
// Operation.
type OperationFunc func(context.Context, *Session) error

// Do implements Operation interface.
func (f OperationFunc) Do(ctx context.Context, s *Session) error {
	return f(ctx, s)
}

// Retryer contains logic of retrying operations failed with retriable errors.
type Retryer struct {
	// SessionProvider is an interface capable for management of ydb sessions.
	// SessionProvider must not be nil.
	SessionProvider SessionProvider

	// MaxRetries is a number of maximum attempts to retry a failed operation.
	// If MaxRetries is zero then no attempts will be made.
	MaxRetries int

	// RetryChecker contains options of mapping errors to retry mode.
	//
	// Note that if RetryChecker's RetryNotFound field is set to true, creation
	// of prepared statements must always be included in the Operation logic.
	// Otherwise when prepared statement become removed by any reason from the
	// server, Retryer will just repeat MaxRetries times reception of statement
	// not found error.
	RetryChecker ydb.RetryChecker

	// Backoff is a selected backoff policy.
	// Deprecated: use FastBackoff and SlowBackoff instead, has no effect now
	Backoff ydb.Backoff

	// FastBackoff is a selected backoff policy.
	// If backoff is nil, then the ydb.DefaultFastBackoff is used.
	FastBackoff ydb.Backoff

	// SlowBackoff is a selected backoff policy.
	// If backoff is nil, then the ydb.DefaultSlowBackoff is used.
	SlowBackoff ydb.Backoff

	// Trace provide tracing of retry logic
	Trace RetryTrace
}

// Retry calls Retryer.Do() configured with default values.
func Retry(ctx context.Context, s SessionProvider, op Operation) error {
	return (Retryer{
		SessionProvider: s,
		MaxRetries:      ydb.DefaultMaxRetries,
		RetryChecker:    ydb.DefaultRetryChecker,
		FastBackoff:     ydb.DefaultFastBackoff,
		SlowBackoff:     ydb.DefaultSlowBackoff,
	}).Do(ctx, op)
}

func (r Retryer) backoff(ctx context.Context, m ydb.RetryMode, i int) error {
	var b ydb.Backoff
	switch m.BackoffType() {
	case ydb.BackoffTypeNoBackoff:
		return nil
	case ydb.BackoffTypeFastBackoff:
		if r.FastBackoff != nil {
			b = r.FastBackoff
		} else {
			b = r.Backoff
		}
	case ydb.BackoffTypeSlowBackoff:
		if r.SlowBackoff != nil {
			b = r.SlowBackoff
		} else {
			b = r.Backoff
		}
	}
	return ydb.WaitBackoff(ctx, b, i)
}

// Do calls op.Do until it return nil or not retriable error.
func (r Retryer) Do(ctx context.Context, op Operation) (err error) {
	var (
		s                     *Session
		m                     ydb.RetryMode
		i                     int
		start                 = time.Now()
		isOperationIdempotent = ydb.IsOperationIdempotent(ctx)
		retryTraceLoopDone    = retryTraceOnLoop(ctx, r.Trace, ctx)
	)
	defer func() {
		retryTraceLoopDone(ctx, time.Since(start), i)
		if s != nil {
			_ = r.SessionProvider.Put(ctx, s)
		}
	}()
	for i = 0; i <= r.MaxRetries; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()

		default:
			if s == nil {
				var e error
				s, e = r.SessionProvider.Get(ctx)
				if s == nil && e == nil {
					panic("only one of pair <session, error> must be not nil")
				}
				if e != nil {
					if err == nil {
						// It is initial attempt to get a Session.
						// Otherwise s could be nil only when status bad session
						// received – that is, we must return bad session error to
						// make it possible to lay on for the client.
						err = e
					}
					return
				}
			}

			if err = op.Do(ctx, s); err == nil {
				return nil
			}

			m = r.RetryChecker.Check(err)
			if m.MustDeleteSession() {
				_ = r.SessionProvider.CloseSession(ctx, s)
				s = nil
			}
			if !m.MustRetry(isOperationIdempotent) {
				return err
			}
			if e := r.backoff(ctx, m, i); e != nil {
				return err
			}
		}
	}
	return err
}

var (
	errNoSession         = errors.New("no session")
	errUnexpectedSession = errors.New("unexpected session")
	errSessionOverflow   = errors.New("session overflow")
)

type singleSession struct {
	s     *Session
	empty bool
}

func (s *singleSession) Get(context.Context) (*Session, error) {
	if s.empty {
		return nil, errNoSession
	}
	s.empty = true
	return s.s, nil
}

func (s *singleSession) Put(_ context.Context, x *Session) error {
	if x != s.s {
		return errUnexpectedSession
	}
	if !s.empty {
		return errSessionOverflow
	}
	s.empty = false
	return nil
}

func (s *singleSession) PutBusy(ctx context.Context, x *Session) error {
	if x != s.s {
		return errUnexpectedSession
	}
	if !s.empty {
		return errSessionOverflow
	}
	s.empty = true
	return x.Close(ctx)
}

func (s *singleSession) CloseSession(ctx context.Context, x *Session) error {
	if x != s.s {
		return errUnexpectedSession
	}
	if !s.empty {
		return errSessionOverflow
	}
	s.empty = true
	return x.Close(ctx)
}
