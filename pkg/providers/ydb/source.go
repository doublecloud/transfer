package ydb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/doublecloud/transfer/library/go/core/metrics"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/format"
	"github.com/doublecloud/transfer/pkg/parsequeue"
	"github.com/doublecloud/transfer/pkg/util"
	"github.com/doublecloud/transfer/pkg/util/jsonx"
	"github.com/doublecloud/transfer/pkg/util/queues/sequencer"
	"github.com/doublecloud/transfer/pkg/util/throttler"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"go.ytsaurus.tech/library/go/core/log"
)

const (
	parallelism            = 10
	bufferFlushingInterval = time.Millisecond * 500
)

type Source struct {
	cfg      *YdbSource
	feedName string
	logger   log.Logger

	once       sync.Once
	ctx        context.Context
	cancelFunc context.CancelFunc

	reader       *readerThreadSafe
	schema       *schemaWrapper
	memThrottler *throttler.MemoryThrottler
	ydbClient    *ydb.Driver

	errCh chan error
}

func (s *Source) Run(sink abstract.AsyncSink) error {
	parseQ := parsequeue.NewWaitable(s.logger, parallelism, sink, s.parse, s.ack)
	defer parseQ.Close()

	return s.run(parseQ)
}

func (s *Source) Stop() {
	s.once.Do(func() {
		s.cancelFunc()
		if err := s.reader.Close(context.Background()); err != nil {
			s.logger.Warn("unable to close reader", log.Error(err))
		}
		if err := s.ydbClient.Close(context.Background()); err != nil {
			s.logger.Warn("unable to close ydb client", log.Error(err))
		}
	})
}

func (s *Source) run(parseQ *parsequeue.WaitableParseQueue[[]batchWithSize]) error {
	defer func() {
		s.Stop()
	}()

	var bufSize uint64
	messagesCount := 0
	var buffer []batchWithSize

	lastPushTime := time.Now()
	for {
		select {
		case <-s.ctx.Done():
			return nil
		case err := <-s.errCh:
			return err
		default:
		}

		if s.memThrottler.ExceededLimits() {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		ydbBatch, err := func() (*topicreader.Batch, error) {
			cloudResolvingCtx, cancel := context.WithTimeout(s.ctx, 10*time.Millisecond)
			defer cancel()
			return s.reader.ReadMessageBatch(cloudResolvingCtx)
		}()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return xerrors.Errorf("read returned error, err: %w", err)
		}
		if ydbBatch != nil {
			batch, err := newBatchWithSize(ydbBatch)
			if err != nil {
				return xerrors.Errorf("unable to read message values: %w", err)
			}
			buffer = append(buffer, batch)

			bufSize += batch.totalSize
			s.memThrottler.AddInflight(batch.totalSize)
			messagesCount += len(ydbBatch.Messages)
		}

		if !s.memThrottler.ExceededLimits() && !(time.Since(lastPushTime) >= bufferFlushingInterval && bufSize > 0) {
			continue
		}

		// send into sink
		s.logger.Info(fmt.Sprintf("begin to process batch: %v items with %v", messagesCount, format.SizeInt(int(bufSize))),
			log.String("offsets", sequencer.BuildMapTopicPartitionToOffsetsRange(batchesToQueueMessages(buffer))))

		if err := parseQ.Add(buffer); err != nil {
			return xerrors.Errorf("unable to add buffer to parse queue: %w", err)
		}

		bufSize = 0
		messagesCount = 0
		buffer = nil
		lastPushTime = time.Now()
	}
}

func (s *Source) parse(buffer []batchWithSize) []abstract.ChangeItem {
	rollbackOnError := util.Rollbacks{}
	defer rollbackOnError.Do()

	rollbackOnError.Add(func() {
		s.memThrottler.ReduceInflight(batchesSize(buffer))
	})

	items := make([]abstract.ChangeItem, 0)
	for _, batch := range buffer {
		for i := range batch.messageValues {
			var event cdcEvent
			// https://st.yandex-team.ru/TM-5444. For type JSON and JSONDocument, YDB returns JSON as an object, not as a string.
			// The CDC format description is somewhat ambiguous, its documentation is https://ydb.tech/en/docs/concepts/cdc#record-structure.
			// The JSON format is described at https://ydb.tech/ru/docs/yql/reference/types/json#utf; however, it is apparently not used in the CDC protocol. As a result, YQL NULL and Json `null` value are represented by the same object in the YQL CDC protocol.
			if err := jsonx.Unmarshal(batch.messageValues[i], &event); err != nil {
				util.Send(s.ctx, s.errCh, xerrors.Errorf("unable to deserialize json, err: %w", err))
				return nil
			}

			msgData := batch.ydbBatch.Messages[i]
			topicPath := msgData.Topic()
			tableName := makeTablePathFromTopicPath(topicPath, s.feedName, s.cfg.Database)
			tableSchema, err := s.getUpToDateTableSchema(tableName, &event)
			if err != nil {
				util.Send(s.ctx, s.errCh, xerrors.Errorf("unable to check table schema, event: %s, err: %w", event.ToJSONString(), err))
				return nil
			}
			item, err := convertToChangeItem(tableName, tableSchema, &event, msgData.WrittenAt, msgData.Offset, msgData.PartitionID(), uint64(len(batch.messageValues[i])), s.fillDefaults())
			if err != nil {
				util.Send(s.ctx, s.errCh, xerrors.Errorf("unable to convert ydb cdc event to changeItem, event: %s, err: %w", event.ToJSONString(), err))
				return nil
			}
			items = append(items, *item)
		}
	}
	rollbackOnError.Cancel()

	return items
}

func (s *Source) ack(buffer []batchWithSize, pushSt time.Time, err error) {
	defer s.memThrottler.ReduceInflight(batchesSize(buffer))

	if err != nil {
		s.logger.Error("failed to push change items", log.Error(err),
			log.String("offsets", sequencer.BuildMapTopicPartitionToOffsetsRange(batchesToQueueMessages(buffer))))
		util.Send(s.ctx, s.errCh, xerrors.Errorf("failed to push change items: %w", err))
		return
	}

	pushed := sequencer.BuildMapTopicPartitionToOffsetsRange(batchesToQueueMessages(buffer))
	s.logger.Info("Got ACK from sink; commiting read messages to the source", log.Duration("delay", time.Since(pushSt)), log.String("pushed", pushed))

	for _, batch := range buffer {
		if err := s.reader.Commit(s.ctx, batch.ydbBatch); err != nil {
			util.Send(s.ctx, s.errCh, xerrors.Errorf("failed to commit change items: %w", err))
			return
		}
	}

	s.logger.Info(
		fmt.Sprintf("Commit messages done in %v", time.Since(pushSt)),
		log.String("pushed", pushed),
	)
}

func (s *Source) updateLocalCacheTableSchema(tablePath string) error {
	tableColumns, err := tableSchema(s.ctx, s.ydbClient, s.cfg.Database, abstract.TableID{Name: tablePath, Namespace: ""})
	if err != nil {
		return xerrors.Errorf("unable to get table schema, table: %s, err: %w", tablePath, err)
	}
	s.schema.Set(tablePath, tableColumns)
	return nil
}

func (s *Source) getUpToDateTableSchema(tablePath string, event *cdcEvent) (*abstract.TableSchema, error) {
	isAllColumnsKnown, err := s.schema.IsAllColumnNamesKnown(tablePath, event)
	if err != nil {
		return nil, xerrors.Errorf("checking table schema returned error, err: %w", err)
	}
	if !isAllColumnsKnown {
		err := s.updateLocalCacheTableSchema(tablePath)
		if err != nil {
			return nil, xerrors.Errorf("unable to update local cache table schema, table: %s, err: %w", tablePath, err)
		}
		isNowAllColumnsKnown, err := s.schema.IsAllColumnNamesKnown(tablePath, event)
		if err != nil {
			return nil, xerrors.Errorf("checking table schema returned error, err: %w", err)
		}
		if !isNowAllColumnsKnown {
			return nil, xerrors.Errorf("changefeed contains unknown column for table: %s", tablePath)
		}
	}
	return s.schema.Get(tablePath), nil
}

func (s *Source) fillDefaults() bool {
	switch s.cfg.ChangeFeedMode {
	case ChangeFeedModeNewImage, ChangeFeedModeNewAndOldImages:
		return true
	}
	return false
}

func batchesToQueueMessages(batches []batchWithSize) []sequencer.QueueMessage {
	messages := make([]sequencer.QueueMessage, 0)
	for _, batch := range batches {
		for _, msg := range batch.ydbBatch.Messages {
			messages = append(messages, sequencer.QueueMessage{
				Topic:     msg.Topic(),
				Partition: int(msg.PartitionID()),
				Offset:    msg.Offset,
			})
		}
	}
	return messages
}

func batchesSize(buffer []batchWithSize) uint64 {
	var size uint64
	for _, batch := range buffer {
		size += batch.totalSize
	}

	return size
}

func NewSource(transferID string, cfg *YdbSource, logger log.Logger, _ metrics.Registry) (*Source, error) {
	clientCtx, cancelFunc := context.WithCancel(context.Background())
	var rb util.Rollbacks
	defer rb.Do()
	rb.Add(cancelFunc)

	ydbClient, err := newYDBSourceDriver(clientCtx, cfg)
	if err != nil {
		return nil, xerrors.Errorf("unable to create ydb, err: %w", err)
	}
	rb.Add(func() { _ = ydbClient.Close(context.Background()) })

	feedName := transferID
	if cfg.ChangeFeedCustomName != "" {
		feedName = cfg.ChangeFeedCustomName
	}
	consumerName := dataTransferConsumerName
	if cfg.ChangeFeedCustomConsumerName != "" {
		consumerName = cfg.ChangeFeedCustomConsumerName
	}

	reader, err := newReader(feedName, consumerName, cfg.Database, cfg.Tables, ydbClient, logger)
	if err != nil {
		return nil, xerrors.Errorf("failed to create stream reader: %w", err)
	}
	rb.Add(func() { _ = reader.Close(context.Background()) })

	schema := newSchemaObj()

	src := &Source{
		cfg:          cfg,
		feedName:     feedName,
		logger:       logger,
		once:         sync.Once{},
		ctx:          clientCtx,
		cancelFunc:   cancelFunc,
		errCh:        make(chan error),
		reader:       reader,
		schema:       schema,
		memThrottler: throttler.NewMemoryThrottler(uint64(cfg.BufferSize)),
		ydbClient:    ydbClient,
	}

	for _, tablePath := range cfg.Tables {
		err = src.updateLocalCacheTableSchema(tablePath)
		if err != nil {
			return nil, xerrors.Errorf("unable to get table schema, tablePath: %s, err: %w", tablePath, err)
		}
	}

	rb.Cancel()
	return src, nil
}
