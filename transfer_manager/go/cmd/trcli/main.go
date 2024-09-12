package main

import (
	"os"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/transfer_manager/go/cmd/trcli/activate"
	"github.com/doublecloud/transfer/transfer_manager/go/cmd/trcli/check"
	"github.com/doublecloud/transfer/transfer_manager/go/cmd/trcli/replicate"
	"github.com/doublecloud/transfer/transfer_manager/go/cmd/trcli/upload"
	"github.com/doublecloud/transfer/transfer_manager/go/cmd/trcli/validate"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/cobraaux"
	_ "github.com/doublecloud/transfer/transfer_manager/go/pkg/dataplane"
	"github.com/spf13/cobra"
	zp "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.ytsaurus.tech/library/go/core/log/zap"
)

var (
	defaultLogLevel  = "debug"
	defaultLogConfig = "console"
)

func main() {
	loggerConfig := newLoggerConfig()
	logger.Log = zap.Must(loggerConfig)
	logLevel := defaultLogLevel
	logConfig := defaultLogConfig

	rootCommand := &cobra.Command{
		Use:          "trcli",
		Short:        "Transfer cli",
		Example:      "./trcli help",
		SilenceUsage: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			switch logConfig {
			case "json":
				loggerConfig = zp.NewProductionConfig()
			case "minimal":
				loggerConfig.EncoderConfig = zapcore.EncoderConfig{
					MessageKey: "message",
					LevelKey:   "level",
					// Disable the rest of the fields
					TimeKey:        "",
					NameKey:        "",
					CallerKey:      "",
					FunctionKey:    "",
					StacktraceKey:  "",
					LineEnding:     zapcore.DefaultLineEnding,
					EncodeLevel:    zapcore.CapitalColorLevelEncoder,
					EncodeName:     nil,
					EncodeDuration: nil,
				}
			}
			switch logLevel {
			case "panic":
				loggerConfig.Level.SetLevel(zapcore.PanicLevel)
			case "fatal":
				loggerConfig.Level.SetLevel(zapcore.FatalLevel)
			case "error":
				loggerConfig.Level.SetLevel(zapcore.ErrorLevel)
			case "warning":
				loggerConfig.Level.SetLevel(zapcore.WarnLevel)
			case "info":
				loggerConfig.Level.SetLevel(zapcore.InfoLevel)
			case "debug":
				loggerConfig.Level.SetLevel(zapcore.DebugLevel)
			default:
				return xerrors.Errorf("unsupported value \"%s\" for --log-level", logLevel)
			}

			logger.Log = zap.Must(loggerConfig)
			return nil
		},
	}
	cobraaux.RegisterCommand(rootCommand, activate.ActivateCommand())
	cobraaux.RegisterCommand(rootCommand, check.CheckCommand())
	cobraaux.RegisterCommand(rootCommand, replicate.ReplicateCommand())
	cobraaux.RegisterCommand(rootCommand, upload.UploadCommand())
	cobraaux.RegisterCommand(rootCommand, validate.ValidateCommand())

	rootCommand.PersistentFlags().StringVar(&logLevel, "log-level", defaultLogLevel, "Specifies logging level for output logs (\"panic\", \"fatal\", \"error\", \"warning\", \"info\", \"debug\")")
	rootCommand.PersistentFlags().StringVar(&logConfig, "log-config", defaultLogConfig, "Specifies logging config for output logs (\"console\", \"json\", \"minimal\")")

	err := rootCommand.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func newLoggerConfig() zp.Config {
	cfg := logger.DefaultLoggerConfig(zapcore.DebugLevel)
	cfg.OutputPaths = []string{"stderr"}
	cfg.ErrorOutputPaths = []string{"stderr"}
	return cfg
}
