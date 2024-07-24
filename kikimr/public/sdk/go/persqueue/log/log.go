package log

import "context"

type Level uint8

const (
	LevelTrace Level = 6 - iota
	LevelDebug
	LevelInfo
	LevelWarn
	LevelError
	LevelFatal
	LevelNone
)

// Logger is a wrapper for ydb-go-sdk logger.
//
// This interface is made for unification with existing loggers.
type Logger interface {
	Log(ctx context.Context, lvl Level, message string, data map[string]interface{})
	With(fieldName string, fieldValue interface{}) Logger
}
