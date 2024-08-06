package postgres

import (
	"context"

	"github.com/doublecloud/tross/transfer_manager/go/pkg/util"
	"github.com/jackc/pgx/v4"
	"go.ytsaurus.tech/library/go/core/log"
)

type pgxLogger struct {
	logger log.Logger
}

func (p pgxLogger) Log(_ context.Context, level pgx.LogLevel, msg string, data map[string]interface{}) {
	var params []log.Field
	for k, v := range data {
		if k == "sql" {
			// https://github.com/doublecloud/tross/arcadia/vendor/github.com/jackc/pgx/v4/conn.go?rev=r9171541#L413
			query, ok := v.(string)
			if ok {
				v = util.DefaultSample(query)
			}
		}
		params = append(params, log.Any(k, v))
	}
	switch level {
	case pgx.LogLevelInfo:
		// Info level is too verbose, hide it behind debug
		p.logger.Debug(msg, params...)
	case pgx.LogLevelWarn:
		p.logger.Warn(msg, params...)
	case pgx.LogLevelError:
		params = append(params, log.Any("callstack", util.GetCurrentGoroutineCallstack()))
		p.logger.Error(msg, params...)
	case pgx.LogLevelDebug:
		p.logger.Debug(msg, params...)
	}
}

func WithLogger(connConfig *pgx.ConnConfig, lgr log.Logger) *pgx.ConnConfig {
	connConfig.Logger = pgxLogger{logger: lgr}
	return connConfig
}
