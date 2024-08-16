package persqueue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue/genproto/Ydb_PersQueue_V0"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue/log"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue/session"
	"google.golang.org/grpc/status"
)

const (
	maxInflightReads       = 3
	lockRequestChannelSize = 4096
	readPollInterval       = 100 * time.Millisecond
)

var (
	ErrResponseIsEmpty        = errors.New("persqueue: received empty response")
	ErrResponseEmptyError     = errors.New("persqueue: received empty error")
	ErrResponseEmptyInit      = errors.New("persqueue: received empty init")
	ErrResponseEmptyData      = errors.New("persqueue: received empty data")
	ErrResponseEmptyBatch     = errors.New("persqueue: received empty batch")
	ErrResponseEmptyLock      = errors.New("persqueue: received empty lock")
	ErrResponseEmptyRelease   = errors.New("persqueue: received empty release")
	ErrResponseEmptyCommit    = errors.New("persqueue: received empty commit")
	ErrResponseEmptyMetadata  = errors.New("persqueue: received message with empty metadata")
	ErrResponseEmptyInitReply = errors.New("persqueue: received message with empty init reply")
)

// InternalSessionHook is used for testing.
var InternalSessionHook = func(*session.Session) {}

type reader struct {
	options ReaderOptions
	logger  log.Logger

	session *session.Session

	commitBuffer *commitBuffer

	stream Ydb_PersQueue_V0.PersQueueService_ReadSessionClient

	c chan Event

	lock     sync.Mutex
	done     chan struct{}
	err      error
	shutdown bool

	lockC chan lockRequest

	// atomic fields
	bytesRead      uint64
	bytesExtracted uint64
	activeReads    int64
}

func (r *reader) C() <-chan Event {
	return r.c
}

func (r *reader) Shutdown() {
	r.lock.Lock()
	r.shutdown = true
	r.lock.Unlock()
}

func (r *reader) checkShutdown() bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.shutdown
}

func (r *reader) Closed() <-chan struct{} {
	return r.done
}

func (r *reader) Err() error {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.err
}

func (r *reader) finish(err error) {
	if r.session != nil {
		_ = r.session.Close()
	}

	r.lock.Lock()
	defer r.lock.Unlock()

	select {
	case <-r.done:
		return

	default:
		r.err = err
		close(r.done)

		// r must be closed last.
		close(r.c)
	}
}

func (r *reader) Stat() Stat {
	st := Stat{
		MemUsage:       r.commitBuffer.Size(),
		InflightCount:  r.commitBuffer.InflightMessages(),
		WaitAckCount:   r.commitBuffer.NumInflightCommits(),
		BytesExtracted: atomic.LoadUint64(&r.bytesExtracted),
		BytesRead:      atomic.LoadUint64(&r.bytesRead),
	}
	if r.session != nil {
		st.SessionID = r.session.SessionID
	}
	return st
}

func (r *reader) Start(ctx context.Context) (*ReaderInit, error) {
	bo := backoff.NewExponentialBackOff()
	for {
		init, err := r.init(ctx)
		if err != nil {
			if nerr, ok := err.(temparable); err != context.DeadlineExceeded && ok && nerr.Temporary() && r.options.RetryOnFailure {
				r.logger.Log(ctx, log.LevelWarn, "Retry start error", map[string]interface{}{
					"error": err.Error(),
				})
				time.Sleep(bo.NextBackOff())
				continue
			}

			r.finish(err)
			return nil, err
		}

		go r.run(ctx)
		return init, nil
	}
}

func (r *reader) sessionOptions() session.Options {
	return session.Options{
		Endpoint:        r.options.Endpoint,
		Port:            r.options.Port,
		Credentials:     r.options.Credentials,
		TLSConfig:       r.options.TLSConfig,
		Logger:          r.options.Logger,
		Database:        r.options.Database,
		ClientTimeout:   r.options.ClientTimeout,
		DiscoverCluster: false, // Note(dbeliakov): Cluster discovery for reader currently not supported by sdk
	}.WithProxy(r.options.Proxy())
}

func (r *reader) init(ctx context.Context) (init *ReaderInit, err error) {
	r.activeReads = 0

	if r.session != nil {
		if closeErr := r.session.Close(); closeErr != nil {
			r.logger.Log(ctx, log.LevelWarn, "Session close error before init", map[string]interface{}{
				"error": closeErr.Error(),
			})
		}
	}

	r.session, err = session.Dial(ctx, r.sessionOptions())
	if err != nil {
		if statusErr, ok := status.FromError(err); ok {
			return nil, &GRPCError{Status: statusErr.Code(), Description: statusErr.Message()}
		}
		return nil, err
	}

	defer func() {
		if err != nil {
			if closeErr := r.session.Close(); closeErr != nil {
				r.logger.Log(ctx, log.LevelWarn, "Session close error", map[string]interface{}{
					"error": closeErr.Error(),
				})
			}
			r.session = nil
		}
	}()

	InternalSessionHook(r.session)

	r.stream, err = r.session.Client.ReadSession(ctx)
	if err != nil {
		return nil, err
	}

	req := &Ydb_PersQueue_V0.ReadRequest_Init{
		Topics:                   r.options.GetTopics(),
		ReadOnlyLocal:            r.options.ReadOnlyLocal,
		ClientId:                 r.options.Consumer,
		ClientsideLocksAllowed:   r.options.ManualPartitionAssignment,
		BalancePartitionRightNow: r.options.ForceRebalance,
		PartitionGroups:          r.options.GetPartitionGroups(),
		MaxReadMessagesCount:     r.options.MaxReadMessagesCount,
		MaxReadSize:              r.options.MaxReadSize,
		MaxReadPartitionsCount:   r.options.MaxReadPartitionsCount,
		CommitsDisabled:          r.options.CommitsDisabled,
		Version:                  Version,
		ProxyCookie:              r.session.Cookie,
		ProtocolVersion:          2, // last version
	}

	if r.options.MaxTimeLag != 0 {
		req.MaxTimeLagMs = uint32(r.options.MaxTimeLag / time.Millisecond)
	}

	if !r.options.ReadTimestamp.IsZero() {
		req.ReadTimestampMs = ToTimestamp(r.options.ReadTimestamp)
	}

	err = r.stream.SendMsg(&Ydb_PersQueue_V0.ReadRequest{
		Request:     &Ydb_PersQueue_V0.ReadRequest_Init_{Init: req},
		Credentials: r.session.GetCredentials(ctx),
	})

	if err != nil {
		if statusErr, ok := status.FromError(err); ok {
			return nil, &GRPCError{Status: statusErr.Code(), Description: statusErr.Message()}
		}
		return nil, err
	}

	rsp := Ydb_PersQueue_V0.ReadResponse{}
	if err := r.stream.RecvMsg(&rsp); err != nil {
		if statusErr, ok := status.FromError(err); ok {
			return nil, &GRPCError{Status: statusErr.Code(), Description: statusErr.Message()}
		}
		return nil, err
	}

	if rsp.Response == nil {
		return nil, ErrResponseIsEmpty
	}

	switch m := rsp.Response.(type) {
	case *Ydb_PersQueue_V0.ReadResponse_Error:
		if m.Error == nil {
			return nil, ErrResponseEmptyError
		}

		return nil, &Error{int(m.Error.Code), m.Error.Description}

	case *Ydb_PersQueue_V0.ReadResponse_Init_:
		if m.Init == nil {
			return nil, ErrResponseEmptyInit
		}

		r.session.SessionID = m.Init.SessionId
		r.logger.Log(ctx, log.LevelInfo, "Reader session initialized", map[string]interface{}{
			"sessionID": m.Init.SessionId,
		})
		return &ReaderInit{SessionID: m.Init.SessionId}, nil

	default:
		return nil, fmt.Errorf("persqueue: received unexpected message type %T", m)
	}
}

func (r *reader) emit(ctx context.Context, e Event) {
	// Better not to process event for sure if context is canceled.
	if ctx.Err() != nil {
		return
	}

	select {
	case r.c <- e:
	case <-ctx.Done():
	}
}

func (r *reader) run(ctx context.Context) {
	defer r.finish(nil)

	for {
		senderErrC := make(chan error, 1)
		receiverErrC := make(chan error, 1)

		go func() {
			err := r.runSender(ctx)
			_ = r.session.Close()
			senderErrC <- err
		}()

		go func() {
			err := r.runReceiver(ctx)
			// Need to keep err == nil only for shutdown.
			if err != nil {
				_ = r.session.Close()
			}
			// Once receiver closed we know that there is no active reads, so reset them
			atomic.StoreInt64(&r.activeReads, 0)
			receiverErrC <- err
		}()

		var err error
		select {
		case err = <-senderErrC:
			<-receiverErrC
			if err == nil {
				// Sender decided to terminate clearly.
				return
			}

		case err = <-receiverErrC:
			if senderErr := <-senderErrC; senderErr == nil {
				// Sender decided to terminate clearly.
				return
			}
		}

		if r.options.RetryOnFailure {
			r.commitBuffer = newCommitBuffer(r.options.MaxMemory)
			r.lockC = make(chan lockRequest, lockRequestChannelSize)

			bo := backoff.NewExponentialBackOff()
			for {
				if err := ctx.Err(); err != nil {
					r.logger.Log(ctx, log.LevelInfo, "Reader is canceled", nil)
					r.finish(err)
					return
				}

				r.emit(ctx, &Disconnect{Err: err})

				_, err = r.init(ctx)
				if err != nil {
					r.logger.Log(ctx, log.LevelError, "Reader restart failed", map[string]interface{}{
						"error": err.Error(),
					})
					time.Sleep(bo.NextBackOff())
					continue
				}

				r.logger.Log(ctx, log.LevelError, "Reader session restarted", nil)
				break
			}
		} else {
			r.logger.Log(ctx, log.LevelError, "Reader terminated because of error", map[string]interface{}{
				"error": err.Error(),
			})
			r.finish(err)
			return
		}
	}
}

func (r *reader) runSender(ctx context.Context) error {
	ticker := time.NewTicker(readPollInterval)
	defer ticker.Stop()

	backoffTimer := backoff.NewExponentialBackOff()
	backoffTimer.Reset()
	backoffTimer.MaxElapsedTime = 0
	nextLogDuration := backoffTimer.NextBackOff()
	logTime := time.Now()

	for {
		select {
		case <-ticker.C:
			if time.Since(logTime) > nextLogDuration {
				logTime = time.Now()
				nextLogDuration = backoffTimer.NextBackOff()
				r.logger.Log(ctx, log.LevelDebug, "Sender tick", map[string]interface{}{
					"activeReads": atomic.LoadInt64(&r.activeReads),
					"inLimits":    r.commitBuffer.InLimits(),
				})
			}

			if cookies := r.commitBuffer.PopEnqueuedCommits(); len(cookies) != 0 {
				if err := r.sendCommit(ctx, cookies); err != nil {
					return err
				}
			}

			if r.checkShutdown() && r.commitBuffer.AllCommitsAcked() {
				return nil
			}

			if r.commitBuffer.InLimits() && atomic.LoadInt64(&r.activeReads) < maxInflightReads {
				if r.checkShutdown() {
					continue
				}

				atomic.AddInt64(&r.activeReads, 1)

				if err := r.sendRead(ctx); err != nil {
					return err
				}
			}

		case lock := <-r.lockC:
			if err := r.sendStartRead(ctx, lock.lock, lock.verifyReadOffset, lock.readOffset, lock.commitOffset); err != nil {
				return err
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (r *reader) runReceiver(ctx context.Context) error {
	for ctx.Err() == nil {
		rsp := &Ydb_PersQueue_V0.ReadResponse{}
		if err := r.stream.RecvMsg(rsp); err != nil {
			return err
		}

		if rsp.Response == nil {
			return ErrResponseIsEmpty
		}

		switch m := rsp.Response.(type) {
		case *Ydb_PersQueue_V0.ReadResponse_Data_:
			atomic.AddInt64(&r.activeReads, -1)

			if m.Data == nil {
				return ErrResponseEmptyData
			}

			data, err := r.handleData(ctx, m.Data)
			if err != nil {
				return err
			}

			r.emit(ctx, data)

		case *Ydb_PersQueue_V0.ReadResponse_BatchedData_:
			atomic.AddInt64(&r.activeReads, -1)

			if m.BatchedData == nil {
				return ErrResponseEmptyBatch
			}

			data, err := r.handleBatchedData(ctx, m.BatchedData)
			if err != nil {
				return err
			}

			r.emit(ctx, data)

		case *Ydb_PersQueue_V0.ReadResponse_Error:
			if m.Error == nil {
				return ErrResponseEmptyError
			}

			err := &Error{int(m.Error.Code), m.Error.Description}
			r.logger.Log(ctx, log.LevelError, "Received error message", map[string]interface{}{
				"error": err.Error(),
			})
			return err

		case *Ydb_PersQueue_V0.ReadResponse_Lock_:
			if m.Lock == nil {
				return ErrResponseEmptyLock
			}

			r.emit(ctx, r.handleLock(m.Lock))

		case *Ydb_PersQueue_V0.ReadResponse_Release_:
			if m.Release == nil {
				return ErrResponseEmptyRelease
			}

			r.emit(ctx, r.handleRelease(m.Release))

		case *Ydb_PersQueue_V0.ReadResponse_Commit_:
			if m.Commit == nil {
				return ErrResponseEmptyCommit
			}

			r.emit(ctx, r.handleCommit(ctx, m.Commit))

		default:
			return fmt.Errorf("persqueue: received unexpected message %T", m)
		}
	}
	if r.checkShutdown() {
		return nil
	}
	return ctx.Err()
}

func (r *reader) sendCommit(ctx context.Context, commitCookies []PartitionCookie) error {
	cookies := make([]uint64, len(commitCookies))
	for i, c := range commitCookies {
		cookies[i] = c.PartitionCookie
	}
	r.logger.Log(ctx, log.LevelDebug, "Sending commit", map[string]interface{}{
		"cookies": cookies,
	})
	return r.stream.SendMsg(&Ydb_PersQueue_V0.ReadRequest{
		Request:     &Ydb_PersQueue_V0.ReadRequest_Commit_{Commit: &Ydb_PersQueue_V0.ReadRequest_Commit{Cookie: cookies}},
		Credentials: r.session.GetCredentials(ctx),
	})
}

func (r *reader) sendRead(ctx context.Context) error {
	r.logger.Log(ctx, log.LevelDebug, "Sending read request", nil)
	return r.stream.SendMsg(&Ydb_PersQueue_V0.ReadRequest{
		Request:     &Ydb_PersQueue_V0.ReadRequest_Read_{Read: &Ydb_PersQueue_V0.ReadRequest_Read{}},
		Credentials: r.session.GetCredentials(ctx),
	})
}

func codecFromProto(codec Ydb_PersQueue_V0.ECodec) (Codec, error) {
	switch codec {
	case Ydb_PersQueue_V0.ECodec_RAW:
		return Raw, nil
	case Ydb_PersQueue_V0.ECodec_GZIP:
		return Gzip, nil
	case Ydb_PersQueue_V0.ECodec_ZSTD:
		return Zstd, nil
	default:
		return 0, fmt.Errorf("persqueue: unsupported codec %s", codec)
	}
}

func (r *reader) handleBatchedData(ctx context.Context, d *Ydb_PersQueue_V0.ReadResponse_BatchedData) (*Data, error) {
	var nMessages int
	batches := make([]MessageBatch, len(d.PartitionData))

	for i, p := range d.PartitionData {
		var msgs []ReadMessage

		for _, batch := range p.Batch {
			extraFields := convertMap(batch.ExtraFields)
			batchMsgs := make([]ReadMessage, len(batch.MessageData))

			for j, m := range batch.MessageData {
				codec, err := codecFromProto(m.Codec)
				if err != nil {
					return nil, err
				}

				nMessages++
				batchMsgs[j] = ReadMessage{
					Offset: m.Offset,

					SeqNo:    m.SeqNo,
					SourceID: batch.SourceId,

					Data:  m.Data,
					Codec: codec,

					CreateTime: FromTimestamp(m.CreateTimeMs),
					WriteTime:  FromTimestamp(batch.WriteTimeMs),
					IP:         batch.Ip,

					ExtraFields: extraFields,
				}

				if err = r.unpack(&batchMsgs[j]); err != nil {
					return nil, err
				}
			}

			msgs = append(msgs, batchMsgs...)
		}

		batches[i] = MessageBatch{
			Partition: p.Partition,
			Topic:     p.Topic,
			Messages:  msgs,
		}
	}

	data := &Data{
		batches:         batches,
		commitBuffer:    r.commitBuffer,
		Cookie:          d.Cookie,
		partitionCookie: PartitionCookie{PartitionCookie: d.Cookie},
	}
	r.commitBuffer.AddCookie(data.partitionCookie, data.size())

	r.logger.Log(ctx, log.LevelDebug, "Received data", map[string]interface{}{
		"nBatches":  len(batches),
		"nMessages": nMessages,
		"cookies":   d.Cookie,
	})
	return data, nil
}

func (r *reader) unpack(m *ReadMessage) (err error) {
	atomic.AddUint64(&r.bytesRead, uint64(len(m.Data)))
	if !r.options.DecompressionDisabled && m.Codec != Raw {
		decPool := getDecPool(m.Codec)
		var dec = decPool.Get().(Decompressor)
		m.Data, err = dec.Decompress(m.Data)
		decPool.Put(dec)
		m.Codec = Raw
	}
	atomic.AddUint64(&r.bytesExtracted, uint64(len(m.Data)))
	return
}

func (r *reader) handleData(ctx context.Context, d *Ydb_PersQueue_V0.ReadResponse_Data) (*Data, error) {
	var nMessages int
	batches := make([]MessageBatch, len(d.MessageBatch))
	for i, partition := range d.MessageBatch {
		msgs := make([]ReadMessage, len(partition.Message))
		for j, m := range partition.Message {
			if m.Meta == nil {
				return nil, ErrResponseEmptyMetadata
			}

			codec, err := codecFromProto(m.Meta.Codec)
			if err != nil {
				return nil, err
			}

			nMessages++
			msgs[j] = ReadMessage{
				Offset: m.Offset,

				SeqNo:    m.Meta.SeqNo,
				SourceID: m.Meta.SourceId,

				Data:  m.Data,
				Codec: codec,

				CreateTime: FromTimestamp(m.Meta.CreateTimeMs),
				WriteTime:  FromTimestamp(m.Meta.WriteTimeMs),
				IP:         m.Meta.Ip,

				ExtraFields: convertMap(m.Meta.ExtraFields),
			}

			if err = r.unpack(&msgs[j]); err != nil {
				return nil, err
			}
		}

		batches[i] = MessageBatch{
			Topic:     partition.Topic,
			Partition: partition.Partition,

			Messages: msgs,
		}
	}

	data := &Data{
		batches:         batches,
		commitBuffer:    r.commitBuffer,
		Cookie:          d.Cookie,
		partitionCookie: PartitionCookie{PartitionCookie: d.Cookie},
	}
	r.commitBuffer.AddCookie(data.partitionCookie, data.size())

	r.logger.Log(ctx, log.LevelDebug, "Received data", map[string]interface{}{
		"nBatches":  len(batches),
		"nMessages": nMessages,
		"cookies":   d.Cookie,
	})
	return data, nil
}

func convertMap(m *Ydb_PersQueue_V0.MapType) (items map[string]string) {
	if m == nil || len(m.Items) == 0 {
		return
	}

	items = map[string]string{}
	for _, item := range m.Items {
		items[item.Key] = item.Value
	}
	return
}

func (r *reader) handleLock(m *Ydb_PersQueue_V0.ReadResponse_Lock) Event {
	r.logger.Log(context.Background(), log.LevelDebug, "Received lock", map[string]interface{}{
		"lock": m,
	})

	return &Lock{
		Topic:      m.Topic,
		Partition:  m.Partition,
		EndOffset:  m.EndOffset,
		Generation: m.Generation,
		ReadOffset: m.ReadOffset,

		lockC: r.lockC,
	}
}

func (r *reader) handleRelease(m *Ydb_PersQueue_V0.ReadResponse_Release) Event {
	r.logger.Log(context.Background(), log.LevelDebug, "Received release", map[string]interface{}{
		"release": m,
	})

	return &Release{
		Generation: m.Generation,
		Partition:  m.Partition,
		Topic:      m.Topic,
		CanCommit:  m.CanCommit,
	}
}

func (r *reader) handleCommit(ctx context.Context, m *Ydb_PersQueue_V0.ReadResponse_Commit) Event {
	r.logger.Log(ctx, log.LevelDebug, "Received commit ack", map[string]interface{}{
		"cookies": m.Cookie,
	})

	partitionCookies := make([]PartitionCookie, len(m.Cookie))
	for i, cookie := range m.Cookie {
		r.commitBuffer.AckCommit(PartitionCookie{PartitionCookie: cookie})
		partitionCookies[i] = PartitionCookie{PartitionCookie: cookie}
	}

	return &CommitAck{Cookies: m.Cookie, PartitionCookies: partitionCookies}
}

func (r *reader) sendStartRead(ctx context.Context, lock *Lock, verifyReadOffset bool, readOffset, commitOffset uint64) error {
	r.logger.Log(ctx, log.LevelInfo, "Locking partition", map[string]interface{}{
		"topic":        lock.Topic,
		"partition":    lock.Partition,
		"readOffset":   readOffset,
		"commitOffset": commitOffset,
	})

	return r.stream.SendMsg(&Ydb_PersQueue_V0.ReadRequest{
		Request: &Ydb_PersQueue_V0.ReadRequest_StartRead_{
			StartRead: &Ydb_PersQueue_V0.ReadRequest_StartRead{
				Topic:            lock.Topic,
				Partition:        lock.Partition,
				ReadOffset:       readOffset,
				VerifyReadOffset: verifyReadOffset,
				Generation:       lock.Generation,
				CommitOffset:     commitOffset,
			},
		},
		Credentials: r.session.GetCredentials(ctx),
	})
}

// NewReader creates new reader for topic
func NewReader(options ReaderOptions) Reader {
	lgr := options.Logger
	if lgr == nil {
		lgr = log.NopLogger
	}

	c := &reader{
		logger:  lgr,
		options: options,
		lockC:   make(chan lockRequest, lockRequestChannelSize),
		c:       make(chan Event, 1),
		done:    make(chan struct{}),

		commitBuffer: newCommitBuffer(options.MaxMemory),
	}

	return c
}
