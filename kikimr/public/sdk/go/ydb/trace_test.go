package ydb

import (
	"testing"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/internal/tracetest"
)

func TestDriverTrace(t *testing.T) {
	tracetest.TestSingleTrace(t, DriverTrace{}, "DriverTrace")
}
