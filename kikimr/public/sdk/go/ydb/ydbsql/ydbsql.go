package ydbsql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb"
)

func init() {
	sql.Register("ydb/v2", new(legacyDriver))
}

type legacyDriver struct {
}

func (d *legacyDriver) OpenConnector(name string) (driver.Connector, error) {
	u, err := url.ParseRequestURI(name)
	if err != nil {
		return nil, err
	}
	if err := validateURL(u); err != nil {
		return nil, err
	}
	return Connector(urlConnectorOptions(u)...), nil
}

func (d *legacyDriver) Open(name string) (driver.Conn, error) {
	return nil, ErrDeprecated
}

const (
	urlAuthToken  = "auth-token"
	databaseToken = "database"
)

func urlConnectorOptions(u *url.URL) []ConnectorOption {
	opts := []ConnectorOption{
		WithEndpoint(u.Host),
		WithDatabase(u.Path),
		withSecure(u.Scheme == secureScheme),
		WithCredentials(ydb.AuthTokenCredentials{
			AuthToken: u.Query().Get(urlAuthToken),
		}),
	}
	database := u.Query().Get(databaseToken)
	if database != "" {
		opts = append(opts, WithDatabase(database))
	}
	return opts
}

var (
	secureScheme = "grpcs"
	validSchemas = []string{
		"ydb",
		"grpc",
		secureScheme,
	}
)

func isValidScheme(scheme string) bool {
	for _, s := range validSchemas {
		if s == scheme {
			return true
		}
	}
	return false
}

func validateURL(u *url.URL) error {
	if !isValidScheme(u.Scheme) {
		return fmt.Errorf("malformed source uri: unexpected scheme: %q", u.Scheme)
	}
	if u.Host == "" {
		return fmt.Errorf("malformed source uri: empty host")
	}
	if u.Path == "" && u.Query().Get(databaseToken) == "" {
		return fmt.Errorf("malformed source uri: empty database")
	}

	var withToken bool
	for key := range u.Query() {
		if key == urlAuthToken {
			withToken = true
		}
	}
	if !withToken {
		return fmt.Errorf("malformed source uri: empty token")
	}

	return nil
}
