package logger

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue"
	"github.com/doublecloud/tross/library/go/core/log/zap"
	"github.com/doublecloud/tross/library/go/core/metrics"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/library/go/core/xerrors/multierr"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/util/size"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
	"go.uber.org/zap/zapcore"
)

type topicWriterClient struct {
	db     *ydb.Driver
	writer *topicwriter.Writer
}

var _ Writer = (*topicWriterClient)(nil)
var _ io.Closer = (*topicWriterClient)(nil)

func newTopicWriterClient(cfg *persqueue.WriterOptions) (*topicWriterClient, error) {
	dsn := fmt.Sprintf("grpcs://%s/?database=%s", cfg.Endpoint, cfg.Database)

	db, err := ydb.Open(context.Background(), dsn, ydb.WithTLSConfig(cfg.TLSConfig), ydb.WithCredentials(cfg.Credentials))
	if err != nil {
		return nil, xerrors.Errorf("failed to connect to YDB: " + err.Error())
	}

	writer, err := db.Topic().StartWriter(cfg.Topic)
	if err != nil {
		return nil, xerrors.Errorf("failed to create topic writer: " + err.Error())
	}

	return &topicWriterClient{
		db:     db,
		writer: writer,
	}, nil
}

func (c *topicWriterClient) makeZapCore(registry metrics.Registry) zapcore.Core {
	leakyWriter := NewLeakyWriter(c, registry, LbLogRecordMaxSize)

	truncatingWriter := NewJSONTruncator(leakyWriter, newConsoleLogger(), JSONTruncatorConfig{
		TotalLimit:  LbLogRecordTruncationTotalLimit,
		StringLimit: size.MiB,
		BytesLimit:  size.MiB,
	}, registry)

	level := parseLevel(LogLevel())

	encoder := zapcore.NewJSONEncoder(zap.JSONConfig(level.Log).EncoderConfig)
	writeSyncer := zapcore.Lock(zapcore.AddSync(truncatingWriter))
	levelEnabler := levelEnablerFactory(level.Zap)

	return zapcore.NewCore(encoder, writeSyncer, levelEnabler)
}

// async buffered write, don't wait for all data to be successfully saved
func (c *topicWriterClient) Write(data []byte) (int, error) {
	if err := c.writer.Write(context.Background(), topicwriter.Message{Data: bytes.NewReader(data)}); err != nil {
		return 0, err
	}
	return len(data), nil
}

func (c *topicWriterClient) CanWrite() bool {
	return true
}

func (c *topicWriterClient) Close() error {
	err1 := c.db.Close(context.Background())
	err2 := c.writer.Close(context.Background())

	return multierr.Combine(err1, err2)
}
