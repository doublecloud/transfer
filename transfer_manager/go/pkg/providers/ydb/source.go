package ydb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/doublecloud/tross/library/go/core/metrics"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/format"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/util"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/util/ioreader"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/util/jsonx"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/util/queues/sequencer"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/util/throttler"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"go.ytsaurus.tech/library/go/core/log"
)

type Source struct {
	cfg      *YdbSource
	feedName string
	logger   log.Logger

	once       sync.Once
	ctx        context.Context
	cancelFunc context.CancelFunc
	errCh      chan error

	reader       *readerThreadSafe
	schema       *schemaWrapper
	memThrottler *throttler.MemoryThrottler
	isSending    atomic.Bool
	ydbClient    *ydb.Driver

	storage abstract.Storage
}

func (s *Source) Run(sink abstract.AsyncSink) error {
	defer func() {
		s.Stop()
	}()
	s.isSending.Store(false)
	buffer := make([]*topicreader.Batch, 0)
	bufferChangeItems := make([][]abstract.ChangeItem, 0)
	bufSize := uint64(0)
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
		batch, err := func() (*topicreader.Batch, error) {
			cloudResolvingCtx, cancel := context.WithTimeout(s.ctx, 10*time.Millisecond)
			defer cancel()
			return s.reader.ReadMessageBatch(cloudResolvingCtx)
		}()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return xerrors.Errorf("read returned error, err: %w", err)
		}
		if batch != nil {
			changeItems, size, err := s.toChangeItems(batch)
			if err != nil {
				return xerrors.Errorf("unable to convert batch to changeItems, err: %w", err)
			}

			bufferChangeItems = append(bufferChangeItems, changeItems)
			buffer = append(buffer, batch)
			bufSize += size

			s.memThrottler.AddInflight(size)
		}

		if s.isSending.Load() || len(bufferChangeItems) == 0 { // accumulate
			continue
		}

		// send into sink

		s.logger.Info(
			fmt.Sprintf("begin to process batch: %v items with %v", len(bufferChangeItems), format.SizeInt(int(bufSize))),
			log.String("offsets", sequencer.BuildLogMapPartitionToOffsetsRange(bufferChangeItems)),
		)

		s.isSending.Store(true) // this should be upraised into this goroutine - if move it into processMessages - it's another goroutine, and lead to RC
		go func(buffer [][]abstract.ChangeItem, batch []*topicreader.Batch, bufSize uint64) {
			err := s.processMessages(sink, buffer, batch, bufSize)
			if err != nil {
				select {
				case s.errCh <- xerrors.Errorf("unable to process messages: %w", err):
				case <-s.ctx.Done():
				}
			}
		}(bufferChangeItems, buffer, bufSize)

		bufferChangeItems = make([][]abstract.ChangeItem, 0)
		buffer = make([]*topicreader.Batch, 0)
		bufSize = 0
	}
}

func (s *Source) processMessages(sink abstract.AsyncSink, buffer [][]abstract.ChangeItem, batch []*topicreader.Batch, bufSize uint64) error {
	defer s.isSending.Store(false)
	changeItems := flatten(buffer)
	pushSt := time.Now()
	if err := <-sink.AsyncPush(changeItems); err != nil {
		s.logger.Error("failed to push changeitems", log.Error(err), log.String("offsets", sequencer.BuildLogMapPartitionToOffsetsRange(buffer)))
		return xerrors.Errorf("failed to push changeitems: %w", err)
	}
	for _, currBatch := range batch {
		err := s.reader.Commit(s.ctx, currBatch)
		if err != nil {
			return xerrors.Errorf("failed to commit changeitems: %w", err)
		}
	}
	s.logger.Info(
		fmt.Sprintf("Commit messages done in %v", time.Since(pushSt)),
		log.String("pushed", sequencer.BuildLogMapPartitionToOffsetsRange(buffer)),
	)
	s.memThrottler.ReduceInflight(bufSize)
	return nil
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

func (s *Source) updateLocalCacheTableSchema(tablePath string) error {
	tableColumns, err := s.storage.TableSchema(s.ctx, abstract.TableID{Name: tablePath, Namespace: ""})
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

func (s *Source) toChangeItems(batch *topicreader.Batch) ([]abstract.ChangeItem, uint64, error) {
	items := make([]abstract.ChangeItem, 0, len(batch.Messages))
	sumSize := uint64(0)
	for _, msg := range batch.Messages {
		ioReader := ioreader.NewCalcSizeWrapper(msg)
		var event cdcEvent
		// https://st.yandex-team.ru/TM-5444. For type JSON and JSONDocument, YDB returns JSON as an object, not as a string.
		// The CDC format description is somewhat ambiguous, its documentation is https://ydb.tech/en/docs/concepts/cdc#record-structure.
		// The JSON format is described at https://ydb.tech/ru/docs/yql/reference/types/json#utf; however, it is apparently not used in the CDC protocol. As a result, YQL NULL and Json `null` value are represented by the same object in the YQL CDC protocol.
		if err := jsonx.NewDefaultDecoder(msg).Decode(&event); err != nil {
			return nil, 0, xerrors.Errorf("unable to deserialize json, err: %w", err)
		}
		sumSize += ioReader.Counter
		topicPath := msg.Topic()
		tableName := makeTablePathFromTopicPath(topicPath, s.feedName, s.cfg.Database)
		tableSchema, err := s.getUpToDateTableSchema(tableName, &event)
		if err != nil {
			return nil, 0, xerrors.Errorf("unable to check table schema, event: %s, err: %w", event.ToJSONString(), err)
		}
		item, err := convertToChangeItem(tableName, tableSchema, &event, msg.WrittenAt, msg.Offset, msg.PartitionID(), ioReader.Counter, s.fillDefaults())
		if err != nil {
			return nil, 0, xerrors.Errorf("unable to convert ydb cdc event to changeItem, event: %s, err: %w", event.ToJSONString(), err)
		}
		items = append(items, *item)
	}
	return items, sumSize, nil
}

func NewSource(transferID string, cfg *YdbSource, logger log.Logger, _ metrics.Registry) (*Source, error) {
	if len(cfg.Tables) == 0 {
		return nil, xerrors.Errorf("unable to replicate all tables in the database")
	}

	clientCtx, cancelFunc := context.WithCancel(context.Background())
	var rb util.Rollbacks
	defer rb.Do()
	rb.Add(cancelFunc)

	ydbClient, err := newClient2(clientCtx, cfg)
	if err != nil {
		return nil, xerrors.Errorf("unable to create ydb, err: %w", err)
	}
	rb.Add(func() { _ = ydbClient.Close(context.Background()) })

	feedName := transferID
	if cfg.ChangeFeedCustomName != "" {
		feedName = cfg.ChangeFeedCustomName
	}
	reader, err := newReader(feedName, cfg.Tables, ydbClient, logger)
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
		isSending:    atomic.Bool{},
		ydbClient:    ydbClient,
		storage:      nil,
	}

	storage, err := NewStorage(cfg.ToStorageParams())
	if err != nil {
		return nil, xerrors.Errorf("unable to create storage, err: %w", err)
	}
	src.storage = storage

	for _, tablePath := range cfg.Tables {
		err = src.updateLocalCacheTableSchema(tablePath)
		if err != nil {
			return nil, xerrors.Errorf("unable to get table schema, tablePath: %s, err: %w", tablePath, err)
		}
	}

	rb.Cancel()
	return src, nil
}
