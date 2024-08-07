package log

import (
	"os"

	"github.com/mattn/go-isatty"
	zp "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/zap"
)

var Log *zap.Logger

func init() {
	encoder := zapcore.CapitalColorLevelEncoder
	if !isatty.IsTerminal(os.Stdout.Fd()) {
		encoder = zapcore.CapitalLevelEncoder
	}
	Log = zap.Must(zp.Config{
		Level:            zp.NewAtomicLevelAt(zapcore.InfoLevel),
		Encoding:         "console",
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
		EncoderConfig: zapcore.EncoderConfig{
			MessageKey:     "msg",
			LevelKey:       "level",
			TimeKey:        "ts",
			CallerKey:      "caller",
			EncodeLevel:    encoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
	})
}

func Warnf(msg string, args ...interface{}) {
	Log.Warnf(msg, args...)
}

func Info(msg string, fields ...log.Field) {
	Log.Info(msg, fields...)
}

func Infof(msg string, args ...interface{}) {
	Log.Infof(msg, args...)
}

func Error(msg string, fields ...log.Field) {
	Log.Error(msg, fields...)
}

func Errorf(msg string, args ...interface{}) {
	Log.Errorf(msg, args...)
}

func Fatalf(msg string, args ...interface{}) {
	Log.Fatalf(msg, args...)
}
