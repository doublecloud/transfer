package table

import (
	"context"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/internal"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/internal/result"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"
)

// Result is a result of a query.
//
// Use NextResultSet(), NextRow() and Scan() to advance through the result sets,
// its rows and row's items.
//
//	res, err := s.Execute(ctx, txc, "SELECT ...")
//	defer res.Close()
//	for res.NextResultSet() {
//	    for res.NextRow() {
//	        var id int64
//	        var name *string //optional value
//	        res.Scan(&id,&name)
//	    }
//	}
//	if err := res.Err() { // get any error encountered during iteration
//	    // handle error
//	}
//
// If current value under scan
// is not requested type, then res.Err() become non-nil.
// After that, NextResultSet(), NextRow() will return false.
type Result struct {
	result.Scanner

	sets    []*Ydb.ResultSet
	nextSet int

	stats *Ydb_TableStats.QueryStats

	setCh       chan *Ydb.ResultSet
	setChErr    *error
	setChCancel func()

	err    error
	closed bool
}

// Stats returns query execution stats.
func (r *Result) Stats() (stats QueryStats) {
	stats.init(r.stats)
	return
}

// SetCount returns number of result sets.
// Note that it does not work if r is the result of streaming operation.
func (r *Result) SetCount() int {
	return len(r.sets)
}

// RowCount returns the number of rows among the all result sets.
// Deprecated: RowCount with multiple result sets from different queries are misleads
// Instead - must iterate over result sets and ask SetRowCount() for each current result set
func (r *Result) RowCount() (n int) {
	for _, s := range r.sets {
		n += len(s.Rows)
	}
	return
}

// SetRowCount returns number of rows in the current result set.
func (r *Result) SetRowCount() int {
	return r.Scanner.RowCount()
}

// SetRowItemCount returns number of items in the current row.
func (r *Result) SetRowItemCount() int {
	return r.Scanner.ItemCount()
}

// Close closes the Result, preventing further iteration.
func (r *Result) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.setCh != nil {
		r.setChCancel()
	}
	return nil
}

// Err return scanner error
// To handle errors, do not need to check after scanning each row
// It is enough to check after reading all ResultSet
func (r *Result) Err() error {
	if r.err != nil {
		return r.err
	}
	return r.Scanner.Err()
}

func (r *Result) inactive() bool {
	return r.closed || r.err != nil || r.Scanner.Err() != nil
}

// Deprecated: Use HasNextResultSet
func (r *Result) HasNextSet() bool {
	return r.HasNextResultSet()
}

// HasNextResultSet reports whether result set may be advanced.
//
// It may be useful to call HasNextResultSet() instead of NextResultSet() to look ahead
// without advancing the result set.
//
// Note that it does not work with sets from stream.
func (r *Result) HasNextResultSet() bool {
	if r.inactive() || r.nextSet == len(r.sets) {
		return false
	}
	return true
}

// Deprecated: Use only NextResultSet with context
func (r *Result) NextSet(columns ...string) bool {
	if !r.HasNextResultSet() {
		return false
	}
	result.Reset(&r.Scanner, r.sets[r.nextSet], columns...)
	r.nextSet++
	return true
}

// NextResultSet selects next result set in the result.
// columns - names of columns in the resultSet that will be scanned
// It returns false if there are no more result sets.
// Stream sets are supported.
func (r *Result) NextResultSet(ctx context.Context, columns ...string) bool {
	if !r.HasNextResultSet() {
		return r.nextStreamSet(ctx, columns...)
	}
	result.Reset(&r.Scanner, r.sets[r.nextSet], columns...)
	r.nextSet++
	return true
}

// Truncated returns true if current result set has been truncated by server.
func (r *Result) Truncated() bool {
	return r.Scanner.ResultSetTruncated()
}

// Deprecated: Use only NextResultSet with context
func (r *Result) NextStreamSet(ctx context.Context, columns ...string) bool {
	return r.nextStreamSet(ctx, columns...)
}

// NextStreamSet selects next result set from the result of streaming operation.
// columns - names of columns in the resultSet that will be scanned
// It returns false if stream is closed or ctx is canceled.
// Note that in case of context cancelation it marks via error set.
func (r *Result) nextStreamSet(ctx context.Context, columns ...string) bool {
	if r.inactive() || r.setCh == nil {
		return false
	}
	select {
	case s, ok := <-r.setCh:
		if !ok {
			if r.setChErr != nil {
				r.err = *r.setChErr
			}
			return false
		}
		result.Reset(&r.Scanner, s, columns...)
		return true

	case <-ctx.Done():
		if r.err == nil {
			r.err = ctx.Err()
		}
		result.Reset(&r.Scanner, nil)
		return false
	}
}

// Columns allows to iterate over all columns of the current result set.
func (r *Result) Columns(it func(Column)) {
	result.Columns(&r.Scanner, func(name string, typ internal.T) {
		it(Column{
			Name: name,
			Type: typ,
		})
	})
}
