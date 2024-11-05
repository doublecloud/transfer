package main

import (
	"net/http"
	"os"

	"github.com/doublecloud/transfer/cmd/trcli/activate"
	"github.com/doublecloud/transfer/cmd/trcli/check"
	"github.com/doublecloud/transfer/cmd/trcli/replicate"
	"github.com/doublecloud/transfer/cmd/trcli/upload"
	"github.com/doublecloud/transfer/cmd/trcli/validate"
	"github.com/doublecloud/transfer/internal/logger"
	internal_metrics "github.com/doublecloud/transfer/internal/metrics"
	"github.com/doublecloud/transfer/library/go/core/metrics"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	coordinator "github.com/doublecloud/transfer/pkg/abstract/coordinator"
	"github.com/doublecloud/transfer/pkg/cobraaux"
	"github.com/doublecloud/transfer/pkg/coordinator/s3coordinator"
	_ "github.com/doublecloud/transfer/pkg/dataplane"
	"github.com/doublecloud/transfer/pkg/serverutil"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	zp "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/zap"
)

var (
	defaultLogLevel    = "debug"
	defaultLogConfig   = "console"
	defaultCoordinator = "memory"
)

func main() {
	var rt abstract.LocalRuntime
	var cp coordinator.Coordinator = coordinator.NewStatefulFakeClient()

	loggerConfig := newLoggerConfig()
	logger.Log = zap.Must(loggerConfig)
	logLevel := defaultLogLevel
	logConfig := defaultLogConfig
	coordinatorTyp := defaultCoordinator
	coordinatorS3Bucket := ""
	runProfiler := false

	rootCommand := &cobra.Command{
		Use:          "trcli",
		Short:        "Transfer cli",
		Example:      "./trcli help",
		SilenceUsage: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if runProfiler {
				go serverutil.RunPprof()
			}

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

			switch coordinatorTyp {
			case defaultCoordinator:
				cp = coordinator.NewStatefulFakeClient()
				if rt.CurrentJob > 0 || rt.ShardingUpload.JobCount > 1 {
					return xerrors.Errorf("for sharding upload memory coordinator won't work")
				}
			case "s3":
				var err error
				cp, err = s3coordinator.NewS3(coordinatorS3Bucket)
				if err != nil {
					return xerrors.Errorf("unable to load s3 coordinator: %w", err)
				}
			}
			return nil
		},
	}

	go serverutil.RunHealthCheck()

	registry := InitPrometheusMetrics()
	cobraaux.RegisterCommand(rootCommand, activate.ActivateCommand(&cp, &rt, registry))
	cobraaux.RegisterCommand(rootCommand, check.CheckCommand())
	cobraaux.RegisterCommand(rootCommand, replicate.ReplicateCommand(&cp, &rt, registry))
	cobraaux.RegisterCommand(rootCommand, upload.UploadCommand(&cp, &rt, registry))
	cobraaux.RegisterCommand(rootCommand, validate.ValidateCommand())

	rootCommand.PersistentFlags().StringVar(&logLevel, "log-level", defaultLogLevel, "Specifies logging level for output logs (\"panic\", \"fatal\", \"error\", \"warning\", \"info\", \"debug\")")
	rootCommand.PersistentFlags().StringVar(&logConfig, "log-config", defaultLogConfig, "Specifies logging config for output logs (\"console\", \"json\", \"minimal\")")
	rootCommand.PersistentFlags().StringVar(&coordinatorTyp, "coordinator", defaultCoordinator, "Specifies how to coordinate transfer nodes (\"memory\", \"s3\")")
	rootCommand.PersistentFlags().StringVar(&coordinatorS3Bucket, "coordinator-s3-bucket", "", "Bucket for s3 coordinator")
	rootCommand.PersistentFlags().BoolVar(&runProfiler, "run-profiler", true, "Run go pprof for performance profiles on 8080 port")
	rootCommand.PersistentFlags().IntVar(&rt.CurrentJob, "coordinator-job-index", 0, "Worker job index")
	rootCommand.PersistentFlags().IntVar(&rt.ShardingUpload.JobCount, "coordinator-job-count", 0, "Worker job count, if more then 1 - run consider as sharded, coordinator is required to be non memory")
	rootCommand.PersistentFlags().IntVar(&rt.ShardingUpload.ProcessCount, "coordinator-process-count", 1, "Worker process count, how many readers must be opened for each job, default is 1")

	err := rootCommand.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func newLoggerConfig() zp.Config {
	cfg := logger.DefaultLoggerConfig(zapcore.DebugLevel)
	cfg.OutputPaths = []string{"stdout"}
	cfg.ErrorOutputPaths = []string{"stderr"}
	return cfg
}

func InitPrometheusMetrics() metrics.Registry {
	promRegistry, m := internal_metrics.NewPrometheusRegistryWithNameProcessor()

	go func() {
		rootMux := http.NewServeMux()
		rootMux.Handle("/metrics", promhttp.HandlerFor(promRegistry, promhttp.HandlerOpts{
			ErrorHandling: promhttp.PanicOnError,
		}))
		logger.Log.Infof("Prometheus is uprising on port %v", "9091")
		if err := http.ListenAndServe(":9091", rootMux); err != nil {
			logger.Log.Error("failed to serve health check", log.Error(err))
		}
	}()

	return m
}
