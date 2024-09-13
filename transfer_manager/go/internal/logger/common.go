package logger

import (
	"os"

	"github.com/doublecloud/transfer/transfer_manager/go/pkg/instanceutil"
	zp "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/zap"
)

type LogLevelSetter struct {
	LogLevel string `yaml:"log_level"`
}

func LogLevel() string {
	if os.Getenv("LOG_LEVEL") != "" {
		return os.Getenv("LOG_LEVEL")
	}
	userData := new(LogLevelSetter)
	if err := instanceutil.GetGoogleCEUserData(userData); err == nil && userData.LogLevel != "" {
		return userData.LogLevel
	}
	return "INFO"
}

func newLogger(core zapcore.Core) log.Logger {
	return &zap.Logger{
		L: zp.New(
			core,
			zp.AddCaller(),
			zp.AddCallerSkip(1),
			zp.AddStacktrace(zp.WarnLevel),
		),
	}
}

func levelEnablerFactory(zapLvl zapcore.Level) zapcore.LevelEnabler {
	return zp.LevelEnablerFunc(func(l zapcore.Level) bool {
		return l >= zapLvl
	})
}

func copySlice(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func copyBytes(source []byte) []byte {
	dup := make([]byte, len(source))
	copy(dup, source)
	return dup
}

func newConsoleLogger() log.Logger {
	levels := getEnvLogLevels()
	if os.Getenv("CONSOLE_LOG_LEVEL") != "" {
		levels = parseLevel(os.Getenv("CONSOLE_LOG_LEVEL"))
	}

	levelEnabler := levelEnablerFactory(levels.Zap)

	encoderConfig := zap.CLIConfig(levels.Log).EncoderConfig
	encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder

	encoder := zapcore.NewConsoleEncoder(encoderConfig)
	writeSyncer := zapcore.AddSync(os.Stdout)

	return newLogger(zapcore.NewCore(encoder, writeSyncer, levelEnabler))
}
