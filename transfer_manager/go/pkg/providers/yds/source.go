package yds

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/log/corelogadapter"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/tross/library/go/core/metrics"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/format"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/functions"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/parsequeue"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/parsers"
	gp "github.com/doublecloud/tross/transfer_manager/go/pkg/parsers/generic"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/logbroker/queues"
	ydbcommon "github.com/doublecloud/tross/transfer_manager/go/pkg/providers/ydb"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/stats"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/util"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/xtls"
	"go.ytsaurus.tech/library/go/core/log"
)

type Source struct {
	config           *YDSSource
	parser           parsers.Parser
	offsetsValidator *queues.LbOffsetsSourceValidator
	consumer         persqueue.Reader
	cancel           context.CancelFunc

	onceStop sync.Once
	stopCh   chan bool

	onceErr sync.Once
	errCh   chan error // buffered channel for exactly one (first) error (width=1)

	metrics *stats.SourceStats
	logger  log.Logger

	executor *functions.Executor
}

func (p *Source) Run(sink abstract.AsyncSink) error {
	parseWrapper := func(buffer []*persqueue.Data) []abstract.ChangeItem {
		if len(buffer) == 0 {
			return []abstract.ChangeItem{abstract.MakeSynchronizeEvent()}
		}
		transformFunc := func(data []abstract.ChangeItem) []abstract.ChangeItem {
			if p.executor != nil {
				st := time.Now()
				p.logger.Infof("begin transform for batches %v rows", len(data))
				transformed, err := p.executor.Do(data)
				if err != nil {
					p.logger.Errorf("Cloud function transformation error in %v, %v rows -> %v rows, err: %v", time.Since(st), len(data), len(transformed), err)
					p.onceErr.Do(func() {
						p.errCh <- err
					})
					return nil
				}
				p.logger.Infof("Cloud function transformation done in %v, %v rows -> %v rows", time.Since(st), len(data), len(transformed))
				p.metrics.TransformTime.RecordDuration(time.Since(st))
				return transformed
			} else {
				return data
			}
		}
		return queues.Parse(buffer, p.parser, p.metrics, p.logger, transformFunc)
	}
	parseQ := parsequeue.NewWaitable(p.logger, 10, sink, parseWrapper, p.ack)
	defer parseQ.Close()

	return p.run(parseQ)
}

func (p *Source) run(parseQ *parsequeue.WaitableParseQueue[[]*persqueue.Data]) error {
	defer func() {
		p.consumer.Shutdown()
		p.logger.Info("Start gracefully close lb reader")
		p.waitSkippedMsgs()
	}()

	lastPush := time.Now()
	bufferSize := 0

	var buffer []*persqueue.Data
	for {
		select {
		case <-p.stopCh:
			p.logger.Warn("Reader closed")
			return nil

		case err := <-p.errCh:
			p.logger.Error("consumer error", log.Error(err))
			return err

		case b, ok := <-p.consumer.C():
			if !ok {
				p.logger.Warn("Reader closed")
				return xerrors.New("consumer closed, close subscription")
			}

			stat := p.consumer.Stat()
			p.metrics.Usage.Set(float64(stat.MemUsage))
			p.metrics.Read.Set(float64(stat.BytesRead))
			p.metrics.Extract.Set(float64(stat.BytesExtracted))

			switch v := b.(type) {
			case *persqueue.CommitAck:
				p.logger.Infof("Ack: %v", v.Cookies)
			case *persqueue.LockV1:
				p.lockPartition(v)
			case *persqueue.ReleaseV1:
				p.logger.Infof("Received 'Release' event, partition:%s@%d", v.Topic, v.Partition)
				err := p.sendSynchronizeEventIfNeeded(parseQ)
				if err != nil {
					return xerrors.Errorf("unable to send synchronize event, err: %w", err)
				}
				v.Release()
			case *persqueue.Disconnect:
				if v.Err != nil {
					p.logger.Errorf("Disconnected: %s", v.Err.Error())
				} else {
					p.logger.Error("Disconnected")
				}
				err := p.sendSynchronizeEventIfNeeded(parseQ)
				if err != nil {
					return xerrors.Errorf("unable to send synchronize event, err: %w", err)
				}
			case *persqueue.Data:
				err := p.offsetsValidator.CheckLbOffsets(v.Batches())
				if err != nil {
					if p.config.AllowTTLRewind {
						p.logger.Warn("ttl rewind", log.Error(err))
					} else {
						p.metrics.Fatal.Inc()
						return abstract.NewFatalError(err)
					}
				}
				ranges := queues.BuildMapPartitionToLbOffsetsRange(v.Batches())
				p.logger.Debug("got lb_offsets", log.Any("range", ranges))

				p.metrics.Master.Set(1)
				buffer = append(buffer, v)
				for _, batch := range v.Batches() {
					for _, m := range batch.Messages {
						bufferSize += len(m.Data)
						p.metrics.Size.Add(int64(len(m.Data)))
						p.metrics.Count.Inc()
					}
				}
			}
		default:
			if len(buffer) == 0 {
				continue
			}
			if p.config.Transformer != nil {
				if time.Since(lastPush).Nanoseconds() < p.config.Transformer.BufferFlushInterval.Nanoseconds() &&
					bufferSize < int(p.config.Transformer.BufferSize) {
					continue
				} else {
					if time.Since(lastPush) < 500*time.Millisecond {
						continue
					}
				}
			}
			p.logger.Infof("begin to process batch: %v items with %v, time from last batch: %v", len(buffer), format.SizeInt(bufferSize), time.Since(lastPush))
			if err := parseQ.Add(buffer); err != nil {
				return xerrors.Errorf("unable to add message to parser process: %w", err)
			}
			lastPush = time.Now()
			buffer = make([]*persqueue.Data, 0)
			bufferSize = 0
		}
	}
}

func (p *Source) waitSkippedMsgs() {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	for {
		select {
		case m := <-p.consumer.C():
			switch v := m.(type) {
			case *persqueue.Data:
				p.logger.Info("skipped message data messages", log.Any("cookie", v.Cookie))
				for _, b := range v.Batches() {
					for _, msg := range b.Messages {
						p.logger.Infof("message: %v@%v at %v", b.Topic, b.Partition, msg.Offset)
					}
				}
			case *persqueue.Disconnect:
				if v.Err != nil {
					p.logger.Infof("Disconnected: %v", v.Err.Error())
				} else {
					p.logger.Info("Disconnected")
				}
			}
		case <-p.consumer.Closed():
			p.logger.Info("Gracefully closed")
			return
		case <-shutdownCtx.Done():
			p.logger.Warn("Timeout while waiting for graceful yds reader shutdown", log.Any("reader_stat", p.consumer.Stat()))
			return
		}
	}
}

func (p *Source) Stop() {
	p.onceStop.Do(func() {
		close(p.stopCh)
		p.cancel()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			p.logger.Warn("timeout in lb reader abort")
			return
		case <-p.consumer.Closed():
			p.logger.Info("abort lb reader")
			return
		}
	}
}

func (p *Source) Fetch() ([]abstract.ChangeItem, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		b, ok := <-p.consumer.C()
		if !ok {
			return nil, xerrors.New("consumer closed, close subscription")
		}
		select {
		case <-ctx.Done():
			return nil, xerrors.New("context deadline")
		default:
		}
		switch v := b.(type) {
		case *persqueue.CommitAck:
			p.logger.Infof("Ack: %v", v.Cookies)
		case *persqueue.LockV1:
			p.lockPartition(v)
		case *persqueue.ReleaseV1:
			_ = p.sendSynchronizeEventIfNeeded(nil)
		case *persqueue.Data:
			var dataBatches [][]abstract.ChangeItem
			batchSize := 0
			var res []abstract.ChangeItem
			var data []abstract.ChangeItem
			for _, b := range v.Batches()[:1] {
				total := len(b.Messages)
				if len(b.Messages) > 3 {
					total = 3
				}
				for _, m := range b.Messages[:total] {
					data = append(data, queues.MessageAsChangeItem(m, b))
					batchSize += len(m.Data)
				}
				res = append(res, data...)
				dataBatches = append(dataBatches, data)
			}
			if p.executor != nil {
				res = nil
				for i := range dataBatches {
					transformed, err := p.executor.Do(dataBatches[i])
					if err != nil {
						return nil, err
					}
					dataBatches[i] = transformed
					res = append(res, transformed...)
				}
			}
			if p.parser != nil {
				res = nil
				// DO CONVERT
				for i := range dataBatches {
					var rows []abstract.ChangeItem
					for _, row := range dataBatches[i] {
						ci, part := queues.ChangeItemAsMessage(row)
						rows = append(rows, p.parser.Do(ci, part)...)
					}
					res = append(res, rows...)
				}
			}
			return res, nil
		case *persqueue.Disconnect:
			if v.Err != nil {
				p.logger.Errorf("Disconnected: %s", v.Err.Error())
			} else {
				p.logger.Error("Disconnected")
			}
			continue
		default:
			continue
		}
	}
}

func (p *Source) lockPartition(lock *persqueue.LockV1) {
	partName := fmt.Sprintf("%v@%v", lock.Topic, lock.Partition)
	p.logger.Infof("Lock partition:%v ReadOffset:%v, EndOffset:%v", partName, lock.ReadOffset, lock.EndOffset)
	p.offsetsValidator.InitOffsetForPartition(lock.Topic, uint32(lock.Partition), lock.ReadOffset)
	lock.StartRead(true, lock.ReadOffset, lock.ReadOffset)
}

func (p *Source) sendSynchronizeEventIfNeeded(parseQ *parsequeue.WaitableParseQueue[[]*persqueue.Data]) error {
	if p.config.IsLbSink && parseQ != nil {
		p.logger.Info("Sending synchronize event")
		if err := parseQ.Add([]*persqueue.Data{}); err != nil {
			return xerrors.Errorf("unable to add message to parser process: %w", err)
		}
		parseQ.Wait()
		p.logger.Info("Sent synchronize event")
	}
	return nil
}

func (p *Source) ack(data []*persqueue.Data, st time.Time, err error) {
	if err != nil {
		p.onceErr.Do(func() {
			p.errCh <- err
		})
		return
	} else {
		for _, b := range data {
			b.Commit()
		}
		p.metrics.PushTime.RecordDuration(time.Since(st))
	}
}

func NewSourceWithOpts(transferID string, cfg *YDSSource, logger log.Logger, registry metrics.Registry, optFns ...SourceOpt) (*Source, error) {
	srcOpts := new(sourceOpts)
	for _, fn := range optFns {
		srcOpts = fn(srcOpts)
	}

	consumer := cfg.Consumer
	if consumer == "" {
		consumer = transferID
	}
	opts := persqueue.ReaderOptions{
		Credentials:               srcOpts.creds,
		Logger:                    corelogadapter.New(logger),
		Endpoint:                  cfg.Endpoint,
		Port:                      cfg.Port,
		Database:                  cfg.Database,
		ManualPartitionAssignment: true,
		Consumer:                  consumer,
		Topics:                    []persqueue.TopicInfo{{Topic: cfg.Stream}},
		MaxReadSize:               256 * 1024,
		MaxMemory:                 128 * 1024 * 1024,
		RetryOnFailure:            true,
	}
	if cfg.TLSEnalbed {
		tls, err := xtls.FromPath(cfg.RootCAFiles)
		if err != nil {
			return nil, xerrors.Errorf("failed to obtain TLS configuration for cloud: %w", err)
		}
		opts.TLSConfig = tls
	}
	if cfg.Transformer != nil {
		opts.MaxMemory = int(cfg.Transformer.BufferSize * 10)
	}
	c := persqueue.NewReaderV1(opts)
	ctx, cancel := context.WithCancel(context.Background())
	var rb util.Rollbacks
	rb.Add(cancel)
	defer rb.Do()

	if _, err := c.Start(ctx); err != nil {
		logger.Error("failed to start reader", log.Error(err))
		return nil, xerrors.Errorf("failed to start reader: %w", err)
	}

	var executor *functions.Executor
	if cfg.Transformer != nil {
		var err error
		executor, err = functions.NewExecutor(cfg.Transformer, cfg.Transformer.CloudFunctionsBaseURL, functions.YDS, logger, registry)
		if err != nil {
			logger.Error("failed to create a function executor", log.Error(err))
			return nil, xerrors.Errorf("failed to create a function executor: %w", err)
		}
	}

	mtrcs := stats.NewSourceStats(registry)
	parser := srcOpts.parser
	if parser == nil && cfg.ParserConfig != nil {
		var err error
		parser, err = parsers.NewParserFromMap(cfg.ParserConfig, false, logger, mtrcs)
		if err != nil {
			return nil, xerrors.Errorf("unable to make parser, err: %w", err)
		}

		// Dirty hack for back compatibility. yds transfer users (including us)
		// use generic parser name field set from cfg.Stream, but topic parametr
		// was removed from parsers conustructors. therefor, we cast parser to
		// generic parser and set it manually
		// subj: TM-6012
		switch wp := parser.(type) {
		case *parsers.ResourceableParser:
			switch p := wp.Unwrap().(type) {
			case *gp.GenericParser:
				p.SetTopic(cfg.Stream)
			}
		}
	}

	rb.Cancel()
	stopCh := make(chan bool)

	yds := &Source{
		config:           cfg,
		parser:           parser,
		offsetsValidator: queues.NewLbOffsetsSourceValidator(logger),
		consumer:         c,
		cancel:           cancel,
		onceStop:         sync.Once{},
		stopCh:           stopCh,
		onceErr:          sync.Once{},
		errCh:            make(chan error, 1),
		metrics:          mtrcs,
		logger:           logger,
		executor:         executor,
	}

	return yds, nil
}

func NewSource(transferID string, cfg *YDSSource, logger log.Logger, registry metrics.Registry) (*Source, error) {
	if cfg.Credentials == nil {
		var err error
		cfg.Credentials, err = ydbcommon.ResolveCredentials(
			cfg.UserdataAuth,
			string(cfg.Token),
			ydbcommon.JWTAuthParams{
				KeyContent:      cfg.SAKeyContent,
				TokenServiceURL: cfg.TokenServiceURL,
			},
			cfg.ServiceAccountID,
			logger,
		)
		if err != nil {
			return nil, xerrors.Errorf("Cannot create YDB credentials: %w", err)
		}
	}
	return NewSourceWithOpts(transferID, cfg, logger, registry, WithCreds(cfg.Credentials))
}

type sourceOpts struct {
	creds  ydb.Credentials
	parser parsers.Parser
}

type SourceOpt = func(*sourceOpts) *sourceOpts

func WithCreds(creds ydb.Credentials) SourceOpt {
	return func(o *sourceOpts) *sourceOpts {
		o.creds = creds
		return o
	}
}

func WithParser(parser parsers.Parser) SourceOpt {
	return func(o *sourceOpts) *sourceOpts {
		o.parser = parser
		return o
	}
}
