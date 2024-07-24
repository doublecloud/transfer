package ydbsql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb"
)

type RetryConfig struct {
	// MaxRetries is a number of maximum attempts to retry a failed operation.
	// If MaxRetries is zero then no attempts will be made.
	MaxRetries int

	// RetryChecker contains options of mapping errors to retry mode.
	RetryChecker ydb.RetryChecker

	// Backoff is a selected backoff policy.
	// Deprecated: use pair FastBackoff / SlowBackoff instead
	Backoff ydb.Backoff

	// FastBackoff is a selected backoff policy.
	// If backoff is nil, then the ydb.DefaultFastBackoff is used.
	FastBackoff ydb.Backoff

	// SlowBackoff is a selected backoff policy.
	// If backoff is nil, then the ydb.DefaultSlowBackoff is used.
	SlowBackoff ydb.Backoff

	// FastSlot is an init slot for fast retry
	// If FastSlot is zero then the ydb.DefaultFastSlot is used.
	FastSlot time.Duration

	// SlowSlot is as zero then the ydb.DefaultSlowSlot is used.
	SlowSlot time.Duration
}

func backoff(ctx context.Context, m ydb.RetryMode, rc *RetryConfig, i int) error {
	var b ydb.Backoff
	switch m.BackoffType() {
	case ydb.BackoffTypeNoBackoff:
		return nil
	case ydb.BackoffTypeFastBackoff:
		if rc.FastBackoff != nil {
			b = rc.FastBackoff
		} else {
			b = rc.Backoff
		}
	case ydb.BackoffTypeSlowBackoff:
		if rc.SlowBackoff != nil {
			b = rc.SlowBackoff
		} else {
			b = rc.Backoff
		}
	}
	return ydb.WaitBackoff(ctx, b, i)
}

type TxOperationFunc func(context.Context, *sql.Tx) error

// TxDoer contains options for retrying transactions.
type TxDoer struct {
	DB      *sql.DB
	Options *sql.TxOptions

	// RetryConfig allows to override retry parameters from DB.
	RetryConfig *RetryConfig
}

// Do starts a transaction and calls f with it. If f() call returns a retryable
// error, it repeats it accordingly to retry configuration that TxDoer's DB
// driver holds.
//
// Note that callers should mutate state outside of f carefully and keeping in
// mind that f could be called again even if no error returned – transaction
// commitment can be failed:
//
//	var results []int
//	ydbsql.DoTx(ctx, db, TxOperationFunc(func(ctx context.Context, tx *sql.Tx) error {
//	    // Reset resulting slice to prevent duplicates when retry occured.
//	    results = results[:0]
//
//	    rows, err := tx.QueryContext(...)
//	    if err != nil {
//	        // handle error
//	    }
//	    for rows.Next() {
//	        results = append(results, ...)
//	    }
//	    return rows.Err()
//	}))
func (d TxDoer) Do(ctx context.Context, f TxOperationFunc) (err error) {
	var (
		rc    = d.RetryConfig
		i     int
		start = time.Now()
	)
	retryNoIdempotent := ydb.IsOperationIdempotent(ctx)
	if rc == nil {
		rc = &d.DB.Driver().(*Driver).c.retryConfig
	}
	for ; i <= rc.MaxRetries; i++ {
		err = d.do(ctx, f)
		if err == nil {
			return
		}
		m := rc.RetryChecker.Check(unwrapErrBadConn(err))
		if !m.MustRetry(retryNoIdempotent) {
			return fmt.Errorf("tx operation are non-retryable (attempts=%d, latency=%s): %w",
				i+1, time.Since(start).String(), err,
			)
		}
		if e := backoff(ctx, m, rc, i); e != nil {
			break
		}
	}
	return fmt.Errorf("tx operation failed (attempts=%d, latency=%s): %w",
		i+1, time.Since(start).String(), err,
	)
}

func (d TxDoer) do(ctx context.Context, f TxOperationFunc) error {
	tx, err := d.DB.BeginTx(ctx, d.Options)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	err = f(ctx, tx)
	if err != nil {
		return err
	}
	return tx.Commit()
}

// DoTx is a shortcut for calling Do(ctx, f) on initialized TxDoer with DB
// field set to given db.
func DoTx(ctx context.Context, db *sql.DB, f TxOperationFunc) error {
	return (TxDoer{DB: db}).Do(ctx, f)
}
