package fedlog

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/log"
)

const (
	lTRACE = log.TRACE
	lDEBUG = log.DEBUG
	lINFO  = log.INFO
	lWARN  = log.WARN
	lERROR = log.ERROR
	lFATAL = log.FATAL
	lQUIET = log.QUIET
)

func logLevel(ctx context.Context, level log.Level, logger log.Logger, msg string, fields ...log.Field) {
	logger.Log(log.WithLevel(ctx, level), msg, fields...)
}
