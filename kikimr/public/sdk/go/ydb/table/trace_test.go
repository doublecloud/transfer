package table

import (
	"testing"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/internal/tracetest"
)

func TestClientTrace(t *testing.T) {
	tracetest.TestSingleTrace(t, ClientTrace{}, "ClientTrace")
}

func TestRetryTrace(t *testing.T) {
	tracetest.TestSingleTrace(t, RetryTrace{}, "RetryTrace")
}

func TestSessionPoolTrace(t *testing.T) {
	tracetest.TestSingleTrace(t, SessionPoolTrace{}, "SessionPoolTrace")
}
