package ydb

import "context"

// Driver is an interface of YDB driver.
type Driver interface {
	Call(context.Context, Operation) (CallInfo, error)
	StreamRead(context.Context, StreamOperation) (CallInfo, error)
	Close() error
}
