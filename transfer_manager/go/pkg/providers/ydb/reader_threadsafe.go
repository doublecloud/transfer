package ydb

import (
	"context"
	"sync"

	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.ytsaurus.tech/library/go/core/log"
)

type readerThreadSafe struct {
	mutex      sync.Mutex
	readerImpl *topicreader.Reader
}

func (r *readerThreadSafe) ReadMessageBatch(ctx context.Context) (*topicreader.Batch, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.readerImpl.ReadMessagesBatch(ctx)
}

func (r *readerThreadSafe) Commit(ctx context.Context, batch *topicreader.Batch) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.readerImpl.Commit(ctx, batch)
}

func (r *readerThreadSafe) Close(ctx context.Context) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.readerImpl.Close(ctx)
}

func newReader(feedName string, tables []string, ydbClient *ydb.Driver, logger log.Logger) (*readerThreadSafe, error) {
	selectors := make([]topicoptions.ReadSelector, len(tables))
	for i, table := range tables {
		selectors[i] = topicoptions.ReadSelector{
			Path: makeChangeFeedPath(table, feedName),
		}
	}

	readerImpl, err := ydbClient.Topic().StartReader(
		dataTransferConsumerName,
		selectors,
		topicoptions.WithReaderCommitTimeLagTrigger(0),
		topicoptions.WithReaderCommitMode(topicoptions.CommitModeSync),
		topicoptions.WithReaderBatchMaxCount(batchSize),
		topicoptions.WithReaderTrace(trace.Topic{
			OnReaderError: func(info trace.TopicReaderErrorInfo) {
				logger.Errorf("topic reader error: %s", info.Error)
			},
		}),
	)
	if err != nil {
		return nil, xerrors.Errorf("unable to start reader, err: %w", err)
	}

	return &readerThreadSafe{
		mutex:      sync.Mutex{},
		readerImpl: readerImpl,
	}, nil
}
