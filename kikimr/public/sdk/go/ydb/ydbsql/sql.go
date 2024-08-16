package ydbsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/table"
)

var (
	ErrDeprecated          = errors.New("ydbsql: deprecated")
	ErrUnsupported         = errors.New("ydbsql: not supported")
	ErrActiveTransaction   = errors.New("ydbsql: can not begin tx within active tx")
	ErrNoActiveTransaction = errors.New("ydbsql: no active tx to work with")
	ErrResultTruncated     = errors.New("ydbsql: result set has been truncated")

	// Deprecated: not used
	ErrSessionBusy = errors.New("ydbsql: session is busy")
)

// conn is a connection to the ydb.
type conn struct {
	connector *connector     // Immutable and r/o usage.
	session   *table.Session // Immutable and r/o usage.

	idle bool

	closed int32

	tx  *table.Transaction
	txc *table.TransactionControl
}

func (c *conn) setClosed() {
	atomic.StoreInt32(&c.closed, 1)
}

func (c *conn) isClosed() bool {
	return atomic.LoadInt32(&c.closed) != 0
}

func (c *conn) ResetSession(ctx context.Context) error {
	if c.isClosed() {
		return driver.ErrBadConn
	}
	if c.idle {
		return nil
	}
	c.idle = true
	return nil
}

func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if c.isClosed() {
		return nil, driver.ErrBadConn
	}

	s, err := c.session.Prepare(ctx, query)
	if err != nil {
		return nil, mapBadSessionError(err)
	}

	return &stmt{
		conn: c,
		stmt: s,
	}, nil
}

// txIsolationOrControl maps driver transaction options to ydb transaction option or query transaction control.
// This caused by ydb logic that prevents start actual transaction with OnlineReadOnly mode and ReadCommitted
// and ReadUncommitted isolation levels should use tx_control in every query request.
// It returns error on unsupported options.
func txIsolationOrControl(opts driver.TxOptions) (isolation table.TxOption, control []table.TxControlOption, err error) {
	level := sql.IsolationLevel(opts.Isolation)
	switch level {
	case sql.LevelDefault,
		sql.LevelSerializable,
		sql.LevelLinearizable:

		isolation = table.WithSerializableReadWrite()
		return

	case sql.LevelReadUncommitted:
		if opts.ReadOnly {
			control = []table.TxControlOption{
				table.BeginTx(
					table.WithOnlineReadOnly(
						table.WithInconsistentReads(),
					),
				),
				table.CommitTx(),
			}
			return
		}

	case sql.LevelReadCommitted:
		if opts.ReadOnly {
			control = []table.TxControlOption{
				table.BeginTx(
					table.WithOnlineReadOnly(),
				),
				table.CommitTx(),
			}
			return
		}
	}
	return nil, nil, fmt.Errorf(
		"ydbsql: unsupported transaction options: isolation=%s read_only=%t",
		nameIsolationLevel(level), opts.ReadOnly,
	)
}

func (c *conn) checkClosed(err error) error {
	if err = mapBadSessionError(err); errors.Is(err, driver.ErrBadConn) {
		c.setClosed()
	}
	return err
}

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (tx driver.Tx, err error) {
	if c.isClosed() {
		return nil, driver.ErrBadConn
	}
	if c.tx != nil || c.txc != nil {
		return nil, ErrActiveTransaction
	}
	isolation, control, err := txIsolationOrControl(opts)
	if err != nil {
		return nil, err
	}
	if isolation != nil {
		c.tx, err = c.session.BeginTransaction(ctx, table.TxSettings(isolation))
		if err != nil {
			return nil, c.checkClosed(err)
		}
		c.txc = table.TxControl(table.WithTx(c.tx))
	} else {
		c.txc = table.TxControl(control...)
	}
	return c, nil
}

// Rollback implements driver.Tx interface.
// Note that it is called by driver even if a user did not called it.
func (c *conn) Rollback() error {
	if c.isClosed() {
		return driver.ErrBadConn
	}

	if c.tx == nil && c.txc == nil {
		return ErrNoActiveTransaction
	}

	tx := c.tx
	c.tx = nil
	c.txc = nil

	if tx != nil {
		return tx.Rollback(context.Background())
	}
	return nil
}

func (c *conn) Commit() error {
	if c.isClosed() {
		return driver.ErrBadConn
	}

	if c.tx == nil && c.txc == nil {
		return ErrNoActiveTransaction
	}

	tx := c.tx
	c.tx = nil
	c.txc = nil

	if tx != nil {
		return tx.Commit(context.Background())
	}
	return nil
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.isClosed() {
		return nil, driver.ErrBadConn
	}
	_, err := c.exec(ctx, &reqQuery{text: query}, params(args))
	if err != nil {
		return nil, err
	}
	return result{}, nil
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (_ driver.Rows, err error) {
	if c.isClosed() {
		return nil, driver.ErrBadConn
	}
	if ContextScanQueryMode(ctx) {
		// Allow to use scanQuery only through QueryContext API.
		return c.scanQueryContext(ctx, query, args)
	}
	return c.queryContext(ctx, query, args)
}

func (c *conn) queryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	res, err := c.exec(ctx, &reqQuery{text: query}, params(args))
	if err != nil {
		return nil, c.checkClosed(err)
	}
	res.NextSet()
	return &rows{res: res}, mapBadSessionError(res.Err())
}

func (c *conn) scanQueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	res, err := c.exec(ctx, &reqScanQuery{text: query}, params(args))
	if err != nil {
		return nil, c.checkClosed(err)
	}
	res.NextStreamSet(ctx)
	return &stream{ctx: ctx, res: res}, mapBadSessionError(res.Err())
}

func (c *conn) CheckNamedValue(v *driver.NamedValue) error {
	return checkNamedValue(v)
}

func (c *conn) Ping(ctx context.Context) error {
	if c.isClosed() {
		return driver.ErrBadConn
	}
	_, err := c.session.KeepAlive(ctx)
	return c.checkClosed(err)
}

func (c *conn) Close() error {
	defer c.setClosed()
	return c.session.Close(context.Background())
}

func (c *conn) Prepare(string) (driver.Stmt, error) {
	return nil, ErrDeprecated
}

func (c *conn) Begin() (driver.Tx, error) {
	return nil, ErrDeprecated
}

func (c *conn) exec(ctx context.Context, req processor, params *table.QueryParameters) (res *table.Result, err error) {
	rc := c.retryConfig()
	retryNoIdempotent := ydb.IsOperationIdempotent(ctx)
	maxRetries := rc.MaxRetries
	if c.tx != nil {
		// NOTE: when under transaction, no retries must be done.
		maxRetries = 0
	}
	var m ydb.RetryMode
	for i := 0; i <= maxRetries; i++ {
		if e := backoff(ctx, m, rc, i-1); e != nil {
			break
		}
		res, err = req.process(ctx, c, params)
		if err == nil {
			return res, nil
		}

		m = rc.RetryChecker.Check(err)

		if m.MustDeleteSession() {
			return nil, c.checkClosed(err)
		}
		if !m.MustRetry(retryNoIdempotent) {
			break
		}
	}
	return nil, c.checkClosed(err)
}

func (c *conn) txControl() *table.TransactionControl {
	if c.txc == nil {
		return c.connector.defaultTxControl
	}
	return c.txc
}

func (c *conn) dataOpts() []table.ExecuteDataQueryOption {
	return c.connector.dataOpts
}

func (c *conn) scanOpts() []table.ExecuteScanQueryOption {
	return c.connector.scanOpts
}

func (c *conn) retryConfig() *RetryConfig {
	return &c.connector.retryConfig
}

type processor interface {
	process(context.Context, *conn, *table.QueryParameters) (*table.Result, error)
}

type reqStmt struct {
	stmt *table.Statement
}

func (o *reqStmt) process(ctx context.Context, c *conn, params *table.QueryParameters) (*table.Result, error) {
	_, res, err := o.stmt.Execute(ctx, c.txControl(), params, c.dataOpts()...)
	return res, err
}

type reqQuery struct {
	text string
}

func (o *reqQuery) process(ctx context.Context, c *conn, params *table.QueryParameters) (*table.Result, error) {
	_, res, err := c.session.Execute(ctx, c.txControl(), o.text, params, c.dataOpts()...)
	return res, err
}

type reqScanQuery struct {
	text string
}

func (o *reqScanQuery) process(ctx context.Context, c *conn, params *table.QueryParameters) (*table.Result, error) {
	return c.session.StreamExecuteScanQuery(ctx, o.text, params, c.scanOpts()...)
}

var isolationLevelName = [...]string{
	sql.LevelDefault:         "default",
	sql.LevelReadUncommitted: "read_uncommitted",
	sql.LevelReadCommitted:   "read_committed",
	sql.LevelWriteCommitted:  "write_committed",
	sql.LevelRepeatableRead:  "repeatable_read",
	sql.LevelSnapshot:        "snapshot",
	sql.LevelSerializable:    "serializable",
	sql.LevelLinearizable:    "linearizable",
}

func nameIsolationLevel(x sql.IsolationLevel) string {
	if int(x) < len(isolationLevelName) {
		return isolationLevelName[x]
	}
	return "unknown_isolation"
}

type stmt struct {
	conn *conn
	stmt *table.Statement
}

func (s *stmt) NumInput() int {
	return s.stmt.NumInput()
}

func (s *stmt) Close() error {
	return nil
}

func (s stmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, ErrDeprecated
}

func (s stmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, ErrDeprecated
}

func (s *stmt) CheckNamedValue(v *driver.NamedValue) error {
	return checkNamedValue(v)
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if s.conn.isClosed() {
		return nil, driver.ErrBadConn
	}
	_, err := s.conn.exec(ctx, &reqStmt{stmt: s.stmt}, params(args))
	if err != nil {
		return nil, err
	}
	return result{}, nil
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if s.conn.isClosed() {
		return nil, driver.ErrBadConn
	}
	if ContextScanQueryMode(ctx) {
		// Allow to use scanQuery only through QueryContext API.
		return s.scanQueryContext(ctx, args)
	}
	return s.queryContext(ctx, args)
}

func (s *stmt) queryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	res, err := s.conn.exec(ctx, &reqStmt{stmt: s.stmt}, params(args))
	if err != nil {
		return nil, s.conn.checkClosed(err)
	}
	res.NextSet()
	return &rows{res: res}, mapBadSessionError(res.Err())
}

func (s *stmt) scanQueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	res, err := s.conn.exec(ctx, &reqScanQuery{text: s.stmt.Text()}, params(args))
	if err != nil {
		return nil, s.conn.checkClosed(err)
	}
	res.NextStreamSet(ctx)
	return &stream{ctx: ctx, res: res}, mapBadSessionError(res.Err())
}

func checkNamedValue(v *driver.NamedValue) (err error) {
	if v.Name == "" {
		return fmt.Errorf("ydbsql: only named parameters are supported")
	}

	if valuer, ok := v.Value.(driver.Valuer); ok {
		v.Value, err = valuer.Value()
		if err != nil {
			return fmt.Errorf("ydbsql: driver.Valuer error: %w", err)
		}
	}

	switch x := v.Value.(type) {
	case ydb.Value:
		// OK.

	case valuer:
		// Some ydbsql level types implement valuer interface.
		// Currently it is a date/time types.
		v.Value = x.Value()

	case bool:
		v.Value = ydb.BoolValue(x)
	case int8:
		v.Value = ydb.Int8Value(x)
	case uint8:
		v.Value = ydb.Uint8Value(x)
	case int16:
		v.Value = ydb.Int16Value(x)
	case uint16:
		v.Value = ydb.Uint16Value(x)
	case int32:
		v.Value = ydb.Int32Value(x)
	case uint32:
		v.Value = ydb.Uint32Value(x)
	case int64:
		v.Value = ydb.Int64Value(x)
	case uint64:
		v.Value = ydb.Uint64Value(x)
	case float32:
		v.Value = ydb.FloatValue(x)
	case float64:
		v.Value = ydb.DoubleValue(x)
	case []byte:
		v.Value = ydb.StringValue(x)
	case string:
		v.Value = ydb.UTF8Value(x)
	case [16]byte:
		v.Value = ydb.UUIDValue(x)

	default:
		return fmt.Errorf("ydbsql: unsupported type: %T", x)
	}

	v.Name = "$" + v.Name

	return nil
}

func params(args []driver.NamedValue) *table.QueryParameters {
	if len(args) == 0 {
		return nil
	}
	opts := make([]table.ParameterOption, len(args))
	for i, arg := range args {
		opts[i] = table.ValueParam(
			arg.Name,
			arg.Value.(ydb.Value),
		)
	}
	return table.NewQueryParameters(opts...)
}

type rows struct {
	res *table.Result
}

func (r *rows) Columns() []string {
	var i int
	cs := make([]string, r.res.ColumnCount())
	r.res.Columns(func(m table.Column) {
		cs[i] = m.Name
		i++
	})
	return cs
}

func (r *rows) NextResultSet() error {
	if !r.res.NextSet() {
		return io.EOF
	}
	return nil
}

func (r *rows) HasNextResultSet() bool {
	return r.res.HasNextSet()
}

func (r *rows) Next(dst []driver.Value) error {
	if !r.res.NextRow() {
		return io.EOF
	}
	for i := range dst {
		// NOTE: for queries like "SELECT * FROM xxx" order of columns is
		// undefined.
		if !r.res.NextItem() {
			err := r.res.Err()
			if err == nil {
				err = io.ErrUnexpectedEOF
			}
			return err
		}
		if r.res.IsOptional() {
			r.res.Unwrap()
		}
		if r.res.IsDecimal() {
			b, p, s := r.res.UnwrapDecimal()
			dst[i] = Decimal{
				Bytes:     b,
				Precision: p,
				Scale:     s,
			}
		} else {
			dst[i] = r.res.Any()
		}
	}
	return r.res.Err()
}

func (r *rows) Close() error {
	return r.res.Close()
}

type stream struct {
	res *table.Result
	ctx context.Context
}

func (r *stream) Columns() []string {
	var i int
	cs := make([]string, r.res.ColumnCount())
	r.res.Columns(func(m table.Column) {
		cs[i] = m.Name
		i++
	})
	return cs
}

func (r *stream) Next(dst []driver.Value) error {
	if !r.res.HasNextRow() {
		if !r.res.NextStreamSet(r.ctx) {
			err := r.res.Err()
			if err != nil {
				return err
			}
			return io.EOF
		}
	}
	if !r.res.NextRow() {
		return io.EOF
	}
	for i := range dst {
		// NOTE: for queries like "SELECT * FROM xxx" order of columns is
		// undefined.
		if !r.res.NextItem() {
			err := r.res.Err()
			if err == nil {
				err = io.ErrUnexpectedEOF
			}
			return err
		}
		if r.res.IsOptional() {
			r.res.Unwrap()
		}
		if r.res.IsDecimal() {
			b, p, s := r.res.UnwrapDecimal()
			dst[i] = Decimal{
				Bytes:     b,
				Precision: p,
				Scale:     s,
			}
		} else {
			dst[i] = r.res.Any()
		}
	}
	return r.res.Err()
}

func (r *stream) Close() error {
	return r.res.Close()
}

type result struct{}

func (r result) LastInsertId() (int64, error) { return 0, ErrUnsupported }
func (r result) RowsAffected() (int64, error) { return 0, ErrUnsupported }
