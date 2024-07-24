package ydb

// CallInfo is struct contained information about call
type CallInfo interface {
	EndpointInfo
}

type callInfo struct {
	address string
}

func (c *callInfo) Conn() *conn {
	return nil
}

func (c *callInfo) Address() string {
	if c == nil {
		return ""
	}
	return c.address
}

var _ CallInfo = &callInfo{}
