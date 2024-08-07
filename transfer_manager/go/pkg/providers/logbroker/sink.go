package logbroker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/log/corelogadapter"
	"github.com/doublecloud/tross/library/go/core/metrics"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/format"
	serializer "github.com/doublecloud/tross/transfer_manager/go/pkg/serializer/queue"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/stats"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/util"
	queues "github.com/doublecloud/tross/transfer_manager/go/pkg/util/queues"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/xtls"
	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"go.ytsaurus.tech/library/go/core/log"
)

const defaultInstance = "lbkxt.logbroker.yandex.net"

type WriterConfigFactory = func(config *LbDestination, shard, groupID, topic string, extras map[string]string, logger log.Logger) (*persqueue.WriterOptions, error)
type WriterFactory = func(*persqueue.WriterOptions, log.Logger) (CancelableWriter, error)

type sink struct {
	config     *LbDestination
	logger     log.Logger
	metrics    *stats.SinkerStats
	serializer serializer.Serializer

	// shard string - became part of SourceID
	//
	// Logbroker has 'writer session' entity, it's identified by SourceID.
	// At every moment of time, exists not more than one unique writer session.
	// Every SourceID corresponds one concrete partition number.
	// Map [hash(SourceID)->partition_number] is stored on lb-side forever.
	// So, it doesn't matter which string is in the 'shard' parameter - it needed only for hash generation.
	//
	// Q: Why default 'shard' value is transferID?
	// A: For users, when >1 transfers write in one topic - transferID as 'shard' supports this case out-of-the-box
	//     ('consolidation' of data from many sources into one topic case)
	//
	// Q: When someone may want set 'shard' parameter?
	// A: Every time, when lb-topic changed the number of partitions - to use all partitions,
	//     you need set new 'sharding policy' manually. It's possible only by setting 'shard' value.
	//
	// Beware, when you are changing the 'shard' parameter - it's resharding,
	// on this moment broken guarantee of consistency 'one tableName is into one partition'
	// So, recommended to do it carefully - for example, after consumer read all available data.
	shard string

	// writers - map: groupID -> writer
	// groupID is fqtn() for non-mirroring, and sourceID for mirroring
	// groupID further became sourceID
	// we need it for cases, when every
	writers map[string]CancelableWriter

	resetWritersFlag bool
	usingTopicAPI    bool

	writerConfigFactory WriterConfigFactory
	writerFactory       WriterFactory
}

func DefaultWriterConfigFactory(config *LbDestination, shard, groupID, topic string, extras map[string]string, logger log.Logger) (*persqueue.WriterOptions, error) {
	sourceID := fmt.Sprintf("%v_%v", shard, groupID)

	opts := persqueue.WriterOptions{
		Endpoint:       config.Instance,
		Database:       config.Database,
		Topic:          topic,
		SourceID:       []byte(sourceID),
		Logger:         corelogadapter.NewWithField(logger, "source_id", sourceID),
		RetryOnFailure: true,
		Codec:          persqueue.Gzip,
		ExtraAttrs:     extras,
		Port:           config.Port,
		ClientTimeout:  time.Duration(60 * time.Second),
	}

	logger.Info("try to init persqueue writer",
		log.String("Endpoint", opts.Endpoint),
		log.Int("Port", opts.Port),
		log.String("Database", opts.Database),
		log.String("Topic", opts.Topic),
		log.Any("TLS mode", config.TLS),
		log.String("SourceID", sourceID),
	)

	if config.TLS == EnabledTLS {
		var err error
		opts.TLSConfig, err = xtls.FromPath(config.RootCAFiles)
		if err != nil {
			return nil, xerrors.Errorf("cannot init persqueue writer config without tls: %w", err)
		}
	}
	opts.Credentials = config.Credentials
	return &opts, nil
}

func DefaultWriterFactory(writerOpts *persqueue.WriterOptions, logger log.Logger) (CancelableWriter, error) {
	ctx, cancelWriter := context.WithCancel(context.Background())

	currWriter := persqueue.NewWriter(*writerOpts)
	initWriter, err := currWriter.Init(ctx)
	if err != nil {
		cancelWriter()
		return nil, err
	}

	logger.Info("persqueue writer is initialized",
		log.String("Cluster", initWriter.Cluster),
		log.String("Topic", initWriter.Topic),
		log.UInt64("Partition", initWriter.Partition),
		log.String("SessionID", initWriter.SessionID),
	)

	return &persqueueWriter{
		groupID:    "",
		writer:     currWriter,
		cancelFunc: cancelWriter,
	}, nil
}

func NewYDSWriterFactory(writerOpts *persqueue.WriterOptions, logger log.Logger) (CancelableWriter, error) {
	ctx, cancelYDBOpen := context.WithTimeout(context.TODO(), time.Second*60)
	defer cancelYDBOpen()

	var dsn string
	if writerOpts.TLSConfig != nil {
		dsn = fmt.Sprintf("grpcs://%s/?database=%s", writerOpts.Endpoint, writerOpts.Database)
	} else {
		// in tests we use grpc, not grpcs
		dsn = fmt.Sprintf("grpc://%s/?database=%s", writerOpts.Endpoint, writerOpts.Database)
	}

	db, err := ydbsdk.Open(ctx, dsn, ydbsdk.WithTLSConfig(writerOpts.TLSConfig), ydbsdk.WithCredentials(writerOpts.Credentials))
	if err != nil {
		return nil, errors.New("Could not connect to YDB: " + err.Error())
	}

	// synchronous writer, waits for ack from server in each Write() call
	writer, err := db.Topic().StartWriter(writerOpts.Topic, topicoptions.WithSyncWrite(true))
	if err != nil {
		return nil, errors.New("Failed to create topic writer: " + err.Error())
	}

	return &topicWriter{
		topicWriter: writer,
		cancelFunc:  cancelYDBOpen,
		groupID:     "",
	}, nil
}

func (s *sink) handleResetWorkers(input []abstract.ChangeItem) {
	if len(input) != 0 {
		lastIndex := len(input) - 1
		if !input[lastIndex].IsRowEvent() && !input[lastIndex].IsTxDone() {
			s.logger.Info("found non-row (and non-tx-done) event - reset writers")
			err := s.closeWriters()
			if err != nil {
				s.logger.Errorf("unable to close writers: %s", err)
			}
		}
	}
}

func (s *sink) getInputWithoutSynchronizeEvent(input []abstract.ChangeItem) []abstract.ChangeItem {
	if len(input) != 0 {
		lastIndex := len(input) - 1
		if input[lastIndex].Kind == abstract.SynchronizeKind {
			return input[0:lastIndex]
		}
	}
	return input
}

func (s *sink) sendSerializedMessages(
	timings *queues.TimingsStatCollector,
	tableToMessages map[abstract.TablePartID][]serializer.SerializedMessage,
	extras map[abstract.TablePartID]map[string]string,
) error {
	for currTablePartID, currMessages := range tableToMessages {
		if currTablePartID.IsSystemTable() && !s.config.AddSystemTables {
			continue
		}

		timings.Started(currTablePartID)

		currTopic := queues.GetTopicName(s.config.Topic, s.config.TopicPrefix, currTablePartID)

		currWriter, err := s.findOrCreateWriter(currTablePartID.FqtnWithPartID(), currTopic, extras[currTablePartID])
		if err != nil {
			return xerrors.Errorf("unable to findOrCreateWriter, topic: %s, err: %w", currTopic, err)
		}

		timings.FoundWriter(currTablePartID)

		var curWrittenMessages int
		var curWrittenSize int

		for _, currMessage := range currMessages {
			if err := s.write(currWriter, currMessage.Value); err != nil {
				errorMsg := fmt.Sprintf(
					"cannot write a message from table %s to topic %s: total messages: %d, written messages: %d : %v",
					currTablePartID.Fqtn(), s.config.Topic, len(currMessages), curWrittenMessages, err)

				s.logger.Errorf(errorMsg)
				return xerrors.Errorf(errorMsg)
			}

			curWrittenMessages++
			curWrittenSize += len(currMessage.Value)
		}

		s.logger.Infof(
			"sent %d messages (%s bytes) from table %s to topic '%s'",
			curWrittenMessages,
			format.SizeInt(curWrittenSize),
			currTablePartID.Fqtn(),
			s.config.Topic,
		)
	}

	return nil
}

func (s *sink) sendAndWaitSerializedMessages(
	timings *queues.TimingsStatCollector,
	tableToMessages map[abstract.TablePartID][]serializer.SerializedMessage,
	extras map[abstract.TablePartID]map[string]string,
) error {
	errCh := make(chan error, 1024)
	messagesTotal := 0
	sizeTotal := 0

	syncErr := func() error {
		for currTablePartID, currMessages := range tableToMessages {
			if currTablePartID.IsSystemTable() && !s.config.AddSystemTables {
				continue
			}

			timings.Started(currTablePartID)

			currTopic := queues.GetTopicName(s.config.Topic, s.config.TopicPrefix, currTablePartID)

			currWriter, err := s.findOrCreateWriter(currTablePartID.FqtnWithPartID(), currTopic, extras[currTablePartID])
			if err != nil {
				return xerrors.Errorf("unable to findOrCreateWriter, topic: %s, err: %w", currTopic, err)
			}

			typedWriter, ok := currWriter.(*persqueueWriter)
			if !ok {
				return abstract.NewFatalError(errors.New("using old persqueue API with new topicWriter"))
			}

			timings.FoundWriter(currTablePartID)

			var currWrittenMessages int
			var currSize int
			var writeError error
			for _, currMessage := range currMessages {
				if writeError = s.write(currWriter, currMessage.Value); writeError != nil {
					s.logger.Errorf("cannot write a message from table %s to topic %s: total messages: %d, written messages: %d : %v", currTablePartID.Fqtn(), s.config.Topic, len(currMessages), currWrittenMessages, writeError)
					break
				}
				currWrittenMessages++
				messagesTotal++
				currSize += len(currMessage.Value)
				sizeTotal += len(currMessage.Value)
			}

			go func(tableID abstract.TablePartID, messagesNum int) {
				for i := 0; i < messagesNum; i++ {
					start := time.Now()
					s.logger.Debugf("Wait for ack #%v table %s", i, tableID.Fqtn())
					ack := <-typedWriter.writer.C()
					switch v := ack.(type) {
					case *persqueue.Issue:
						s.metrics.Table(tableID.Fqtn(), "error", 1)
						s.logger.Error("Ack error", log.Error(v.Err))
						errCh <- v.Err
					default:
						s.logger.Debugf("Ack received for ack #%v table %v in %v", i, tableID.Fqtn(), time.Since(start))
						errCh <- nil
					}
				}
				timings.Finished(currTablePartID)
			}(currTablePartID, currWrittenMessages)

			s.logger.Infof(
				"sent %d messages (%s bytes) from table %s to topic '%s'",
				currWrittenMessages,
				format.SizeInt(currSize),
				currTablePartID.Fqtn(),
				s.config.Topic,
			)

			if writeError != nil {
				return xerrors.Errorf("cannot write a message from table %s to topic %s: %w", currTablePartID.Fqtn(), s.config.Topic, writeError)
			}
		}
		return nil
	}()

	// wait finishing asynchronous sending

	readAll := func(in error) error {
		firstError := in
		for i := 0; i < messagesTotal; i++ {
			err := <-errCh
			if err != nil && firstError == nil {
				firstError = err
			}
		}
		return firstError
	}
	return readAll(syncErr)
}

func (s *sink) Push(inputRaw []abstract.ChangeItem) error {
	start := time.Now()

	defer s.handleResetWorkers(inputRaw)
	input := s.getInputWithoutSynchronizeEvent(inputRaw)

	// serialize

	startSerialization := time.Now()
	var tableToMessages map[abstract.TablePartID][]serializer.SerializedMessage
	var extras map[abstract.TablePartID]map[string]string = nil
	var err error
	perTableMetrics := true
	if s.config.FormatSettings.Name == server.SerializationFormatLbMirror { // see comments to the function 'GroupAndSerializeLB'
		// 'id' here - sourceID
		tableToMessages, extras, err = s.serializer.(*serializer.MirrorSerializer).GroupAndSerializeLB(input)
		perTableMetrics = false
	} else {
		// 'id' here - fqtn()
		tableToMessages, err = s.serializer.Serialize(input)
	}
	if err != nil {
		return xerrors.Errorf("unable to serialize: %w", err)
	}
	serializer.LogBatchingStat(s.logger, input, tableToMessages, startSerialization)

	// send asynchronous

	startSending := time.Now()

	timings := queues.NewTimingsStatCollector()

	if s.usingTopicAPI {
		err = s.sendSerializedMessages(timings, tableToMessages, extras)
		if err != nil {
			return xerrors.Errorf("sendSerializedMessages returned error, err: %w", err)
		}
	} else {
		err = s.sendAndWaitSerializedMessages(timings, tableToMessages, extras)
		if err != nil {
			return xerrors.Errorf("sendAndWaitSerializedMessages returned error, err: %w", err)
		}
	}

	// handle metrics & logging
	if perTableMetrics {
		for groupID, currGroup := range tableToMessages {
			s.metrics.Table(groupID.Fqtn(), "rows", len(currGroup))
		}
	}

	s.logger.Info("Sending async timings stat", append([]log.Field{log.String("push_elapsed", time.Since(start).String()), log.String("sending_elapsed", time.Since(startSending).String())}, timings.GetResults()...)...)
	s.metrics.Elapsed.RecordDuration(time.Since(start))
	return nil
}

func (s *sink) findOrCreateWriter(groupID, topic string, extras map[string]string) (CancelableWriter, error) {
	if conn, ok := s.writers[groupID]; ok {
		return conn, nil
	}

	writerOpts, err := s.writerConfigFactory(s.config, s.shard, groupID, topic, extras, s.logger)
	if err != nil {
		return nil, xerrors.Errorf("unable to get WriterOptions: %w", err)
	}

	// writer should be in temporary variable, and should be written in s.writers only after successes Init()
	writer, err := s.writerFactory(writerOpts, s.logger)
	if err != nil {
		return nil, xerrors.Errorf("unable to build writer: %w", err)
	}
	writer.SetGroupID(groupID)

	s.writers[groupID] = writer

	return writer, nil
}

func (s *sink) write(writer CancelableWriter, data []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(s.config.WriteTimeoutSec))
	defer cancel()

	errCh := make(chan error)

	go func() {
		errCh <- writer.Write(ctx, data)
	}()

	for {
		select {
		case <-ctx.Done():
			s.logger.Warnf("Timed out while waiting for Logbroker write to finish for table %s", writer.GroupID())
			writer.Cancel()
			if err := <-errCh; err != nil {
				s.logger.Error("Cannot write message to Logbroker", log.Error(err))
			}
			if err := s.deleteWriter(writer); err != nil {
				s.logger.Errorf("unable to cancel & close writer for table %s, err: %s", writer.GroupID(), err)
			}
			return xerrors.New("LB: sink write timeout exceeded")
		case err := <-errCh:
			return err
		}
	}
}

func (s *sink) deleteWriter(w CancelableWriter) error {
	if w == nil {
		return nil
	}

	if _, ok := s.writers[w.GroupID()]; !ok {
		return xerrors.Errorf("unable to find writer for table: %s, impossible case", w.GroupID())
	}

	delete(s.writers, w.GroupID())

	if err := w.Close(context.TODO()); err != nil && !xerrors.Is(err, context.Canceled) {
		return err
	}

	return nil
}

func (s *sink) closeWriters() error {
	errs := util.NewErrs()

	for _, lb := range s.writers {
		if lb == nil {
			continue
		}
		if err := lb.Close(context.TODO()); err != nil && !xerrors.Is(err, context.Canceled) {
			errs = util.AppendErr(errs, xerrors.Errorf("failed to close Writer: %w", err))
		}
	}

	s.writers = make(map[string]CancelableWriter)

	if len(errs) > 0 {
		return errs
	}

	return nil
}

func (s *sink) Close() error {
	err := s.closeWriters()
	if err != nil {
		return xerrors.Errorf("unable to close writers: %w", err)
	}
	return nil
}

func NewSinkWithFactory(cfg *LbDestination, registry metrics.Registry, lgr log.Logger, transferID string,
	writerConfigFactory WriterConfigFactory, writerFactory WriterFactory, usingTopicAPI, isSnapshot bool) (abstract.Sinker, error) {

	_, err := queues.NewTopicDefinition(cfg.Topic, cfg.TopicPrefix)
	if err != nil {
		return nil, xerrors.Errorf("unable to validate topic settings: %w", err)
	}

	currFormat := cfg.FormatSettings
	if cfg.FormatSettings.Name == server.SerializationFormatDebezium {
		currFormat = serializer.MakeFormatSettingsWithTopicPrefix(currFormat, cfg.TopicPrefix, cfg.Topic)
	}

	currSerializer, err := serializer.New(currFormat, cfg.SaveTxOrder, true, isSnapshot, lgr)
	if err != nil {
		return nil, xerrors.Errorf("unable to create serializer: %w", err)
	}

	snk := sink{
		config:              cfg,
		logger:              lgr,
		metrics:             stats.NewSinkerStats(registry),
		serializer:          currSerializer,
		resetWritersFlag:    false,
		writers:             make(map[string]CancelableWriter),
		shard:               cfg.Shard,
		writerConfigFactory: writerConfigFactory,
		writerFactory:       writerFactory,
		usingTopicAPI:       usingTopicAPI,
	}
	if snk.shard == "" {
		snk.shard = transferID
	}

	if snk.config.Instance == "" {
		snk.config.Instance = defaultInstance
	}

	return &snk, nil
}

func NewReplicationSink(cfg *LbDestination, registry metrics.Registry, lgr log.Logger, transferID string) (abstract.Sinker, error) {
	return NewSinkWithFactory(cfg, registry, lgr, transferID, DefaultWriterConfigFactory, DefaultWriterFactory, false, false)
}

func NewSnapshotSink(cfg *LbDestination, registry metrics.Registry, lgr log.Logger, transferID string) (abstract.Sinker, error) {
	return NewSinkWithFactory(cfg, registry, lgr, transferID, DefaultWriterConfigFactory, DefaultWriterFactory, false, true)
}
