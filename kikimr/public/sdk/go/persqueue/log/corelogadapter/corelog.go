package corelogadapter

import (
	"context"
	"testing"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/log"
	cr "github.com/doublecloud/tross/library/go/core/log"
	"github.com/doublecloud/tross/library/go/core/log/zap"
	zp "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

type adapter struct {
	logger cr.Logger
}

func (a *adapter) Log(_ context.Context, lvl log.Level, message string, data map[string]interface{}) {
	if lvl == log.LevelNone {
		return
	}

	// NOTE(shmel1k@): possible optimization: Do not prepare data if log-level is less than in logger.
	fields := prepareData(data)

	switch lvl {
	case log.LevelTrace:
		a.logger.Trace(message, fields...)
	case log.LevelDebug:
		a.logger.Debug(message, fields...)
	case log.LevelInfo:
		a.logger.Info(message, fields...)
	case log.LevelWarn:
		a.logger.Warn(message, fields...)
	case log.LevelError:
		a.logger.Error(message, fields...)
	case log.LevelFatal:
		a.logger.Fatal(message, fields...)
	}
}

func (a *adapter) With(fieldName string, fieldValue interface{}) log.Logger {
	return &adapter{
		logger: cr.With(a.logger, cr.Any(fieldName, fieldValue)),
	}
}

func New(logger cr.Logger) log.Logger {
	return &adapter{
		logger: logger,
	}
}

func NewWithName(logger cr.Logger, name string) log.Logger {
	return &adapter{
		logger: logger.WithName(name),
	}
}

func NewWithField(logger cr.Logger, fieldName string, fieldValue interface{}) log.Logger {
	return &adapter{
		logger: cr.With(logger, cr.Any(fieldName, fieldValue)),
	}
}

func NewTest(t *testing.T) log.Logger {
	return &adapter{
		logger: &zap.Logger{
			L: zaptest.NewLogger(t),
		},
	}
}

var _ log.Logger = &adapter{}

var DefaultLogger log.Logger = &adapter{
	logger: zap.Must(zp.Config{
		Level:            zp.NewAtomicLevelAt(zp.InfoLevel),
		Encoding:         "console",
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
		EncoderConfig: zapcore.EncoderConfig{
			MessageKey:     "msg",
			LevelKey:       "level",
			TimeKey:        "ts",
			CallerKey:      "caller",
			EncodeLevel:    zapcore.CapitalLevelEncoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
	}),
}

func prepareData(data map[string]interface{}) []cr.Field {
	result := make([]cr.Field, 0, len(data))
	for k, v := range data {
		result = append(result, cr.Any(k, v))
	}
	return result
}
