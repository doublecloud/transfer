package ydb

// Deprecated: from now we use auto policy in cluster.Get()
type ConnUsePolicy uint8

// Deprecated: from now we use auto policy in cluster.Get()
const (
	ConnUseDefault ConnUsePolicy = 1 << iota >> 1
	ConnUseBalancer
	ConnUseEndpoint

	ConnUseSmart = ConnUseBalancer | ConnUseEndpoint
)
