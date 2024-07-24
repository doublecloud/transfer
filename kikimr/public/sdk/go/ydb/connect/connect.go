package connect

import (
	"context"
	"crypto/tls"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/auth"
)

// New connects to database and return database connection
func New(ctx context.Context, params ConnectParams, opts ...ConnectOption) (c *Connection, err error) {
	c = &Connection{
		table:  newTableWrapper(ctx),
		scheme: newSchemeWrapper(ctx),
	}
	for _, opt := range opts {
		err := opt(c)
		if err != nil {
			return nil, err
		}
	}
	if c.driverConfig == nil {
		c.driverConfig = &ydb.DriverConfig{}
	}
	c.driverConfig.Trace = c.driverConfig.Trace.Compose(ydb.ContextDriverTrace(ctx))
	c.driverConfig.Database = params.Database()
	if c.driverConfig.Credentials == nil {
		c.driverConfig.Credentials, err = auth.FromEnviron(ctx)
		if err != nil {
			return nil, err
		}
	}
	var tlsConfig *tls.Config
	if params.UseTLS() {
		tlsConfig = new(tls.Config)
	}
	c.driver, err = (&ydb.Dialer{
		DriverConfig: c.driverConfig,
		TLSConfig:    tlsConfig,
	}).Dial(ctx, params.Endpoint())
	if err != nil {
		return nil, err
	}
	c.table.client.Driver = c.driver
	c.scheme.client.Driver = c.driver
	return c, nil
}
