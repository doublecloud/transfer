package ydb

// EndpointInfo is struct contained information about endpoint
type EndpointInfo interface {
	// Deprecated: using raw conn does not handle endpoint reconnection
	Conn() *conn
	Address() string
}
