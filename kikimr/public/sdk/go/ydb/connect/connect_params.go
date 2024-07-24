package connect

import (
	"errors"
	"fmt"
	"net/url"
)

var (
	_insecureProtocol = "grpc"
	_secureProtocol   = "grpcs"

	validSchemas = map[string]struct{}{
		_insecureProtocol: {},
		_secureProtocol:   {},
	}

	errSchemeNotValid = errors.New("scheme not valid")
)

func hasTLS(schema string) bool {
	return schema == _secureProtocol
}

func parseConnectionString(connection string) (schema string, endpoint string, database string, _ error) {
	uri, err := url.Parse(connection)
	if err != nil {
		return "", "", "", err
	}
	if _, has := validSchemas[uri.Scheme]; !has {
		return "", "", "", fmt.Errorf("%s: %w", uri.Scheme, errSchemeNotValid)
	}
	return uri.Scheme, uri.Host, uri.Query().Get("database"), err
}

type ConnectParams interface {
	Endpoint() string
	Database() string
	UseTLS() bool
}

type connectParams struct {
	endpoint string
	database string
	useTLS   bool
}

func (c connectParams) Endpoint() string {
	return c.endpoint
}

func (c connectParams) Database() string {
	return c.database
}

func (c connectParams) UseTLS() bool {
	return c.useTLS
}

func EndpointDatabase(endpoint string, database string, tls bool) ConnectParams {
	return &connectParams{
		endpoint: endpoint,
		database: database,
		useTLS:   tls,
	}
}

func ConnectionString(uri string) (ConnectParams, error) {
	schema, endpoint, database, err := parseConnectionString(uri)
	if err != nil {
		return nil, err
	}
	return &connectParams{
		endpoint: endpoint,
		database: database,
		useTLS:   hasTLS(schema),
	}, nil
}

func MustConnectionString(uri string) ConnectParams {
	connectionParams, err := ConnectionString(uri)
	if err != nil {
		panic(err)
	}
	return connectionParams
}
