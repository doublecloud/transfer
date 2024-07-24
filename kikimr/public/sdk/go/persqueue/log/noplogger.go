package log

import "context"

type nopLogger struct{}

func (n *nopLogger) Log(_ context.Context, _ Level, _ string, _ map[string]interface{}) {
}

func (n *nopLogger) With(_ string, _ interface{}) Logger {
	return n
}

var NopLogger Logger = &nopLogger{}
