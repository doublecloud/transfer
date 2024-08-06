package logger

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/log/corelogadapter"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/tross/library/go/core/metrics"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/util/size"
	"go.uber.org/zap/zapcore"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/zap"
)

const (
	LbLogRecordTruncationTotalLimit = size.MiB      // after truncation total size can still be greater than limit
	LbLogRecordMaxSize              = 10 * size.MiB // if greater log record is discarded
)

type LogbrokerConfig struct {
	Instance    string
	Token       string
	Topic       string
	SourceID    string
	Credentials ydb.Credentials
	Port        int
}

type unbufferedPusher struct {
	producer  persqueue.Writer
	maxMemory int
}

func (p *unbufferedPusher) CanWrite() bool {
	stat := p.producer.Stat()
	return stat.MemUsage <= p.maxMemory
}

func (p *unbufferedPusher) Write(data []byte) (int, error) {
	msg := persqueue.WriteMessage{
		Data:            copySlice(data),
		CreateTimestamp: time.Now(),
	}
	if err := p.producer.Write(&msg); err != nil {
		return 0, err
	}
	return len(data), nil
}

func (p *unbufferedPusher) Close() error {
	return p.producer.Close()
}

func NewLogbrokerLoggerFromConfig(cfg *LogbrokerConfig, registry metrics.Registry) (log.Logger, error) {
	if cfg.Credentials == nil && cfg.Token != "" {
		cfg.Credentials = ydb.AuthTokenCredentials{AuthToken: cfg.Token}
	}

	lgr, _, err := NewLogbrokerLoggerDeprecated(&persqueue.WriterOptions{
		Endpoint:       cfg.Instance,
		Credentials:    cfg.Credentials,
		Topic:          cfg.Topic,
		Port:           cfg.Port,
		SourceID:       []byte(cfg.SourceID),
		RetryOnFailure: true,
		MaxMemory:      50 * size.MiB,
	}, registry)
	return lgr, err
}

func NewLogbrokerLoggerDeprecated(cfg *persqueue.WriterOptions, registry metrics.Registry) (log.Logger, io.Closer, error) {
	pr := persqueue.NewWriter(*cfg)

	var inited bool
	for i := 0; i <= 3; i++ {
		if _, err := pr.Init(context.Background()); err != nil {
			Log.Warn("failed to start producer", log.Error(err))
			time.Sleep(time.Second)
		} else {
			inited = true
			break
		}
	}

	if !inited {
		return nil, nil, errors.New("lb: unable to start producer")
	}

	consoleCore := consoleLoggingCore()
	consoleLogger := newLogger(consoleCore)

	lbWriter := &unbufferedPusher{
		producer:  pr,
		maxMemory: cfg.MaxMemory,
	}

	lbLeakyWriter := NewLeakyWriter(lbWriter, registry, LbLogRecordMaxSize)
	lbTruncator := NewJSONTruncator(lbLeakyWriter, consoleLogger, JSONTruncatorConfig{
		TotalLimit:  LbLogRecordTruncationTotalLimit,
		StringLimit: size.MiB,
		BytesLimit:  size.MiB,
	}, registry)

	lbSyncWriter := zapcore.AddSync(lbTruncator)

	lbLevel := parseLevel(LogLevel())
	lbPriority := levelEnablerFactory(lbLevel.Zap)

	lbEncoder := zapcore.NewJSONEncoder(zap.JSONConfig(lbLevel.Log).EncoderConfig)
	lbCore := zapcore.NewCore(lbEncoder, zapcore.Lock(lbSyncWriter), lbPriority)

	go persqueue.ReceiveIssues(lbWriter.producer)

	Log.Info("WriterInit LB logger", log.Any("topic", cfg.Topic), log.Any("instance", cfg.Endpoint))
	return newLogger(zapcore.NewTee(consoleCore, lbCore)), lbWriter, nil
}

func consoleLoggingCore() zapcore.Core {
	consoleLevel := getEnvLogLevels()
	if os.Getenv("CONSOLE_LOG_LEVEL") != "" {
		consoleLevel = parseLevel(os.Getenv("CONSOLE_LOG_LEVEL"))
	}
	defaultPriority := levelEnablerFactory(consoleLevel.Zap)

	consoleEncoderConfig := zap.CLIConfig(consoleLevel.Log).EncoderConfig
	consoleEncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	consoleEncoder := zapcore.NewConsoleEncoder(consoleEncoderConfig)

	consoleWriter := zapcore.AddSync(os.Stdout)

	return zapcore.NewCore(consoleEncoder, consoleWriter, defaultPriority)
}

type bufferedPusher struct {
	producer  persqueue.Writer
	rw        sync.Mutex
	buf       bytes.Buffer
	maxMemory int
}

func (f *bufferedPusher) CanWrite() bool {
	stat := f.producer.Stat()
	return stat.MemUsage <= f.maxMemory
}

func (f *bufferedPusher) Write(p []byte) (n int, err error) {
	f.rw.Lock()
	defer f.rw.Unlock()
	f.buf.Write(p)
	n = len(p)
	if f.buf.Len() > 1024 {
		combined := f.buf.Bytes()
		if string(combined)[f.buf.Len()-1] != '\n' {
			return n, nil
		}
		err = f.producer.Write(&persqueue.WriteMessage{Data: copyBytes(combined)})
		if err != nil {
			return n, err
		}
		f.buf.Reset()
	}
	return n, nil
}

func (f *bufferedPusher) Close() error {
	return f.producer.Close()
}

func NewJobLogger(cfg LogbrokerConfig, registry metrics.Registry) (log.Logger, io.Closer, error) {
	jobLevel := getEnvLogLevels()
	defaultPriority := levelEnablerFactory(jobLevel.Zap)

	ytJobLevel := getEnvYtLogLevel()
	ytDefaultPriority := levelEnablerFactory(ytJobLevel.Zap)

	instance := cfg.Instance
	token := cfg.Token
	topic := cfg.Topic
	jobID := cfg.SourceID
	Log.Info("WriterInit LB push client source "+jobID, log.Any("topic", topic))
	inr := Log
	var pr persqueue.Writer

	var inited bool
	for i := 0; i <= 3; i++ {
		pr = persqueue.NewWriter(
			persqueue.WriterOptions{
				Logger:         corelogadapter.New(inr),
				Endpoint:       instance,
				Credentials:    ydb.AuthTokenCredentials{AuthToken: token},
				Topic:          topic,
				SourceID:       []byte(jobID),
				RetryOnFailure: true,
			},
		)
		if _, err := pr.Init(context.Background()); err != nil {
			Log.Warn("Failed to start producer", log.Error(err))
			time.Sleep(time.Second)
		} else {
			inited = true
			break
		}
	}

	if !inited {
		return nil, nil, errors.New("lb: unable to start producer")
	}

	consoleWriter := zapcore.AddSync(os.Stderr)
	consoleEncoder := zapcore.NewConsoleEncoder(zap.CLIConfig(jobLevel.Log).EncoderConfig)
	ytConsoleEncoder := zapcore.NewConsoleEncoder(zap.CLIConfig(ytJobLevel.Log).EncoderConfig)
	consoleCore := zapcore.NewCore(consoleEncoder, consoleWriter, defaultPriority)
	ytConsoleCore := zapcore.NewCore(ytConsoleEncoder, consoleWriter, ytDefaultPriority)
	consoleLogger := newLogger(consoleCore)

	lbWriter := &bufferedPusher{
		producer:  pr,
		rw:        sync.Mutex{},
		buf:       bytes.Buffer{},
		maxMemory: size.GiB,
	}
	lbLeakyWriter := NewLeakyWriter(lbWriter, registry, LbLogRecordMaxSize)
	lbTruncator := NewJSONTruncator(lbLeakyWriter, consoleLogger, JSONTruncatorConfig{
		TotalLimit:  LbLogRecordTruncationTotalLimit,
		StringLimit: size.MiB,
		BytesLimit:  size.MiB,
	}, registry)
	lbSyncLockedWriter := zapcore.Lock(zapcore.AddSync(lbTruncator))
	lbEncoder := zapcore.NewJSONEncoder(zap.JSONConfig(jobLevel.Log).EncoderConfig)
	lbCore := zapcore.NewCore(lbEncoder, lbSyncLockedWriter, defaultPriority)

	lbYtEncoder := zapcore.NewJSONEncoder(zap.JSONConfig(ytJobLevel.Log).EncoderConfig)
	lbYtCore := zapcore.NewCore(lbYtEncoder, lbSyncLockedWriter, ytDefaultPriority)

	go persqueue.ReceiveIssues(lbWriter.producer)

	Log.Info("WriterInit LB logger", log.Any("topic", topic), log.Any("instance", instance))
	regularConsoleAndLbLogger := newLogger(zapcore.NewTee(consoleCore, lbCore))
	ytConsoleAndLbLogger := newLogger(zapcore.NewTee(ytConsoleCore, lbYtCore))
	return NewYtLogBundle(regularConsoleAndLbLogger, ytConsoleAndLbLogger), lbWriter, nil
}

func NewConsoleLogger() log.Logger {
	consoleLevel := getEnvLogLevels()
	defaultPriority := levelEnablerFactory(consoleLevel.Zap)
	syncStderr := zapcore.AddSync(os.Stderr)
	stdErrEncoder := zapcore.NewConsoleEncoder(zap.CLIConfig(consoleLevel.Log).EncoderConfig)
	lbCore := zapcore.NewTee(
		zapcore.NewCore(stdErrEncoder, syncStderr, defaultPriority),
	)

	return newLogger(lbCore)
}
