package ydbsql

import (
	"context"
	"crypto/tls"
	"database/sql/driver"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/table"
)

var (
	DefaultIdleThreshold        = 5 * time.Second
	DefaultSessionPoolSizeLimit = 1 << 12
)

type ConnectorOption func(*connector)

func WithDialer(d ydb.Dialer) ConnectorOption {
	return func(c *connector) {
		c.dialer = d
		if c.dialer.DriverConfig == nil {
			c.dialer.DriverConfig = new(ydb.DriverConfig)
		}
	}
}

func WithClient(client *table.Client) ConnectorOption {
	return func(c *connector) {
		c.mu.Lock()
		c.client = client
		c.mu.Unlock()
	}
}

func WithCreateSessionTimeout(timeout time.Duration) ConnectorOption {
	return func(c *connector) {
		c.mu.Lock()
		c.createSessionTimeout = timeout
		c.mu.Unlock()
	}
}

func WithEndpoint(addr string) ConnectorOption {
	return func(c *connector) {
		c.endpoint = addr
	}
}

func WithDriverConfig(config ydb.DriverConfig) ConnectorOption {
	return func(c *connector) {
		*(c.dialer.DriverConfig) = config
	}
}

func WithCredentials(creds ydb.Credentials) ConnectorOption {
	return func(c *connector) {
		c.dialer.DriverConfig.Credentials = creds
	}
}

func WithDatabase(db string) ConnectorOption {
	return func(c *connector) {
		c.dialer.DriverConfig.Database = db
	}
}

func withSecure(secure bool) ConnectorOption {
	return func(c *connector) {
		if secure {
			c.dialer.TLSConfig = new(tls.Config)
		} else {
			c.dialer.TLSConfig = nil
		}
	}
}

func WithDriverTrace(t ydb.DriverTrace) ConnectorOption {
	return func(c *connector) {
		c.dialer.DriverConfig.Trace = t
	}
}

func WithClientTrace(t table.ClientTrace) ConnectorOption {
	return func(c *connector) {
		c.clientTrace = t
	}
}

// WithSessionPoolTrace
// Deprecated: has no effect, use database/sql session pool management
func WithSessionPoolTrace(t table.SessionPoolTrace) ConnectorOption {
	return func(c *connector) {}
}

// WithSessionPoolSizeLimit
// Deprecated: has no effect, use database/sql session pool management
func WithSessionPoolSizeLimit(n int) ConnectorOption {
	return func(c *connector) {}
}

// WithSessionPoolIdleThreshold
// Deprecated: has no effect, use database/sql session pool management
func WithSessionPoolIdleThreshold(d time.Duration) ConnectorOption {
	return func(c *connector) {}
}

// WithSessionPoolBusyCheckInterval
// Deprecated: has no effect, use database/sql session pool management
func WithSessionPoolBusyCheckInterval(time.Duration) ConnectorOption {
	return func(c *connector) {}
}

// WithSessionPoolKeepAliveBatchSize
// Deprecated: has no effect, use database/sql session pool management
func WithSessionPoolKeepAliveBatchSize(int) ConnectorOption {
	return func(c *connector) {}
}

// WithSessionPoolKeepAliveTimeout
// Deprecated: has no effect, use database/sql session pool management
func WithSessionPoolKeepAliveTimeout(d time.Duration) ConnectorOption {
	return func(c *connector) {}
}

// WithSessionPoolCreateSessionTimeout
// Deprecated: has no effect, use database/sql session pool management
func WithSessionPoolCreateSessionTimeout(d time.Duration) ConnectorOption {
	return func(c *connector) {}
}

// WithSessionPoolDeleteTimeout
// Deprecated: has no effect, use database/sql session pool management
func WithSessionPoolDeleteTimeout(d time.Duration) ConnectorOption {
	return func(c *connector) {}
}

func WithMaxRetries(n int) ConnectorOption {
	return func(c *connector) {
		if n >= 0 {
			c.retryConfig.MaxRetries = n
		}
	}
}

func WithRetryBackoff(b ydb.Backoff) ConnectorOption {
	return func(c *connector) {
		c.retryConfig.Backoff = b
	}
}

func WithRetryFastSlot(fastSlot time.Duration) ConnectorOption {
	return func(c *connector) {
		c.retryConfig.FastSlot = fastSlot
	}
}

func WithRetrySlowSlot(slowSlot time.Duration) ConnectorOption {
	return func(c *connector) {
		c.retryConfig.SlowSlot = slowSlot
	}
}

func WithDefaultTxControl(txControl *table.TransactionControl) ConnectorOption {
	return func(c *connector) {
		c.defaultTxControl = txControl
	}
}

func WithDefaultExecDataQueryOption(opts ...table.ExecuteDataQueryOption) ConnectorOption {
	return func(c *connector) {
		c.dataOpts = append(c.dataOpts, opts...)
	}
}

func WithDefaultExecScanQueryOption(opts ...table.ExecuteScanQueryOption) ConnectorOption {
	return func(c *connector) {
		c.scanOpts = append(c.scanOpts, opts...)
	}
}

var retryChecker = ydb.RetryChecker{
	// NOTE: we do not want to retry not found prepared statement
	// errors.
	//
	// In other case we would just burn the CPU looping up to max
	// retry attempts times – we are not making any Prepare() calls
	// in the retry callbacks.
	RetryNotFound: false,
}

func Connector(opts ...ConnectorOption) driver.Connector {
	c := &connector{
		dialer: ydb.Dialer{
			DriverConfig: new(ydb.DriverConfig),
		},
		retryConfig: RetryConfig{
			MaxRetries:   ydb.DefaultMaxRetries,
			RetryChecker: retryChecker,
			SlowSlot:     ydb.DefaultSlowSlot,
			FastSlot:     ydb.DefaultFastSlot,
		},
		defaultTxControl: table.TxControl(
			table.BeginTx(
				table.WithSerializableReadWrite(),
			),
			table.CommitTx(),
		),
		createSessionTimeout: table.DefaultSessionPoolCreateSessionTimeout,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// USE CONNECTOR ONLY
type connector struct {
	dialer   ydb.Dialer
	endpoint string

	clientTrace table.ClientTrace

	mu                   sync.RWMutex
	client               *table.Client
	createSessionTimeout time.Duration

	retryConfig      RetryConfig
	defaultTxControl *table.TransactionControl

	dataOpts []table.ExecuteDataQueryOption
	scanOpts []table.ExecuteScanQueryOption
}

func (c *connector) init(ctx context.Context) (err error) {
	// in driver database/sql/sql.go:1228 connect run under mutex, but don't rely on it here
	c.mu.Lock()
	defer c.mu.Unlock()
	// in driver database/sql/sql.go:1228 connect run under mutex, but don't rely on it here

	// Setup some more on less generic reasonable pool limit to prevent
	// session overflow on the YDB servers.
	//
	// Note that it must be controlled from outside by making
	// database/sql.DB.SetMaxIdleConns() call. Unfortunately, we can not
	// receive that limit here and we do not want to force user to
	// configure it twice (and pass it as an option to connector).
	if c.client == nil {
		c.client, err = c.dial(ctx)
	}
	return
}

func (c *connector) dial(ctx context.Context) (*table.Client, error) {
	d, err := c.dialer.Dial(ctx, c.endpoint)
	if err != nil {
		if stringer, ok := c.dialer.DriverConfig.Credentials.(fmt.Stringer); ok {
			return nil, fmt.Errorf("dial error: %w (credentials: %s)", err, stringer.String())
		}
		return nil, fmt.Errorf("dial error: %w", err)
	}
	return &table.Client{
		Driver: d,
		Trace:  c.clientTrace,
	}, nil
}

func (c *connector) connectAttempt(ctx context.Context) (s *table.Session, err error) {
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.createSessionTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.createSessionTimeout)
		defer cancel()
	}
	return c.client.CreateSession(ctx)
}

func (c *connector) Connect(ctx context.Context) (_ driver.Conn, err error) {
	if err = c.init(ctx); err != nil {
		return nil, err
	}
	var (
		s *table.Session
	)
	for i := 0; ; i++ {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		s, err = c.connectAttempt(ctx)
		if err == nil {
			if s == nil {
				panic("ydbsql: abnormal result of pool.Create()")
			}
			cc := &conn{
				connector: c,
				session:   s,
			}
			s.OnClose(cc.setClosed)
			return cc, nil
		}
		m := ydb.DefaultRetryChecker.Check(err)
		if !m.MustRetry(true) {
			return nil, err
		}
		if e := backoff(ctx, m, &c.retryConfig, i); e != nil {
			return nil, err
		}
	}
}

func (c *connector) Driver() driver.Driver {
	return &Driver{c}
}

var _ io.Closer = &connector{}

func (c *connector) Close() error {
	d := Driver{c}
	return d.Close()
}

func (c *connector) unwrap(ctx context.Context) (*table.Client, error) {
	if err := c.init(ctx); err != nil {
		return nil, err
	}
	return c.client, nil
}

// Driver is an adapter to allow the use table client as sql.Driver instance.
type Driver struct {
	c *connector
}

func (d *Driver) Close() error {
	d.c.mu.RLock()
	defer d.c.mu.RUnlock()
	if d.c.client == nil {
		return nil
	}
	return d.c.client.Driver.Close()
}

// Open returns a new connection to the ydb.
func (d *Driver) Open(string) (driver.Conn, error) {
	return nil, ErrDeprecated
}

func (d *Driver) OpenConnector(string) (driver.Connector, error) {
	return d.c, nil
}

func (d *Driver) Unwrap(ctx context.Context) (*table.Client, error) {
	return d.c.unwrap(ctx)
}
