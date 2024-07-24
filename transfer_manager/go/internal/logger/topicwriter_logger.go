package logger

import (
	"io"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue"
	"github.com/doublecloud/tross/library/go/core/log"
	"github.com/doublecloud/tross/library/go/core/metrics"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"go.uber.org/zap/zapcore"
)

func NewTopicWriterLogger(cfg *persqueue.WriterOptions, registry metrics.Registry) (log.Logger, io.Closer, error) {
	client, err := newTopicWriterClient(cfg)
	if err != nil {
		return nil, nil, xerrors.Errorf("failed to create topic writer client: %w", err)
	}

	consoleCore := consoleLoggingCore()
	topicWriterCore := client.makeZapCore(registry)
	combinedCore := zapcore.NewTee(consoleCore, topicWriterCore)

	return newLogger(combinedCore), client, nil
}
