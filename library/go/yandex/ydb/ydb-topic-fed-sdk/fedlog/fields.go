package fedlog

import (
	"context"
	"slices"
	"time"

	"github.com/doublecloud/tross/library/go/core/buildinfo"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
)

// latencyField creates Field "latency": time.Since(start)
func latencyField(start time.Time) log.Field {
	return log.Duration("latency", time.Since(start))
}

// versionField creates Field "version": version.Version
func versionField() log.Field {
	versionInfo := buildinfo.Info.SVNRevision
	if versionInfo == "" {
		versionInfo = buildinfo.Info.Hash
	}

	return log.String("version", versionInfo)
}

func with(ctx context.Context, lvl log.Level, names ...string) context.Context {
	return log.WithLevel(log.WithNames(ctx, names...), lvl)
}

func logDone(ctx context.Context, logger log.Logger, err error, okLevel, errLevel log.Level, message string, fields ...log.Field) {
	level := okLevel
	if err != nil {
		level = errLevel
		message += " (failed)"
		fields = append(slices.Clip(fields), log.Error(err)) // slices.Clip for prevent data races
	}

	logger.Log(log.WithLevel(ctx, level), message, fields...)
}
