package persqueue

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/genproto/Ydb_PersQueue_V1"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/log"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/session"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
	"google.golang.org/grpc/status"
)

var InternalSessionHookV1 = func(*session.SessionV1) {}

type readerV1 struct {
	options ReaderOptions
	logger  log.Logger

	session *session.SessionV1

	commitBuffer *commitBuffer

	stream Ydb_PersQueue_V1.PersQueueService_MigrationStreamingReadClient

	c chan Event

	lock     sync.Mutex
	done     chan struct{}
	err      error
	shutdown bool

	lockC    chan lockV1Request
	releaseC chan releaseV1Request

	// atomic fields
	bytesRead      uint64
	bytesExtracted uint64
	activeReads    int64
}

func (r *readerV1) C() <-chan Event {
	return r.c
}

func (r *readerV1) Shutdown() {
	r.lock.Lock()
	r.shutdown = true
	r.lock.Unlock()
}

func (r *readerV1) checkShutdown() bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.shutdown
}

func (r *readerV1) Closed() <-chan struct{} {
	return r.done
}

func (r *readerV1) Err() error {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.err
}

func (r *readerV1) finish(err error) {
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

func (r *readerV1) Stat() Stat {
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

func (r *readerV1) Start(ctx context.Context) (*ReaderInit, error) {
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

func (r *readerV1) sessionOptions() session.Options {
	return session.Options{
		Endpoint:    r.options.Endpoint,
		Port:        r.options.Port,
		Credentials: r.options.Credentials,
		TLSConfig:   r.options.TLSConfig,
		Logger:      r.options.Logger,
		Database:    r.options.Database,
	}.WithProxy(r.options.Proxy())
}

func (r *readerV1) init(ctx context.Context) (init *ReaderInit, err error) {
	r.activeReads = 0

	if r.session != nil {
		if closeErr := r.session.Close(); closeErr != nil {
			r.logger.Log(ctx, log.LevelWarn, "Session was closed before init", map[string]interface{}{
				"error": closeErr.Error(),
			})
		}
	}

	r.session, err = session.DialV1(ctx, r.sessionOptions())
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

	InternalSessionHookV1(r.session)
	ctx, err = r.session.NewOutgoingContext(ctx)
	if err != nil {
		if statusErr, ok := status.FromError(err); ok {
			return nil, &GRPCError{Status: statusErr.Code(), Description: statusErr.Message()}
		}
		return nil, err
	}
	r.stream, err = r.session.Client.MigrationStreamingRead(ctx)
	if err != nil {
		if statusErr, ok := status.FromError(err); ok {
			return nil, &GRPCError{Status: statusErr.Code(), Description: statusErr.Message()}
		}
		return nil, err
	}

	var topics []*Ydb_PersQueue_V1.MigrationStreamingReadClientMessage_TopicReadSettings
	for _, t := range r.options.Topics {
		groupIDS := make([]int64, len(t.PartitionGroups))
		for i, g := range t.PartitionGroups {
			groupIDS[i] = int64(g)
		}
		topics = append(topics, &Ydb_PersQueue_V1.MigrationStreamingReadClientMessage_TopicReadSettings{
			Topic:             t.Topic,
			PartitionGroupIds: groupIDS,
		})
	}
	req := &Ydb_PersQueue_V1.MigrationStreamingReadClientMessage_InitRequest{
		TopicsReadSettings: topics,
		ReadOnlyOriginal:   r.options.ReadOnlyLocal,
		Consumer:           r.options.Consumer,
		ReadParams: &Ydb_PersQueue_V1.ReadParams{
			MaxReadMessagesCount: r.options.MaxReadMessagesCount,
			MaxReadSize:          r.options.MaxReadSize,
		},
	}

	if r.options.MaxTimeLag != 0 {
		req.MaxLagDurationMs = int64(r.options.MaxTimeLag / time.Millisecond)
	}

	if !r.options.ReadTimestamp.IsZero() {
		req.StartFromWrittenAtMs = int64(ToTimestamp(r.options.ReadTimestamp))
	}

	r.logger.Log(ctx, log.LevelInfo, "Init request", map[string]interface{}{
		"request": req,
	})
	err = r.stream.SendMsg(&Ydb_PersQueue_V1.MigrationStreamingReadClientMessage{
		Request: &Ydb_PersQueue_V1.MigrationStreamingReadClientMessage_InitRequest_{
			InitRequest: req,
		},
		Token: r.session.GetCredentials(ctx),
	})

	if err != nil {
		if statusErr, ok := status.FromError(err); ok {
			return nil, &GRPCError{Status: statusErr.Code(), Description: statusErr.Message()}
		}
		return nil, err
	}

	rsp := Ydb_PersQueue_V1.MigrationStreamingReadServerMessage{}
	if err := r.stream.RecvMsg(&rsp); err != nil {
		if statusErr, ok := status.FromError(err); ok {
			return nil, &GRPCError{Status: statusErr.Code(), Description: statusErr.Message()}
		}
		return nil, err
	}

	if rsp.Status != Ydb.StatusIds_SUCCESS {
		return nil, &Error{int(rsp.Status), rsp.Status.String()}
	}

	if rsp.Response == nil {
		return nil, ErrResponseIsEmpty
	}

	switch m := rsp.Response.(type) {
	case *Ydb_PersQueue_V1.MigrationStreamingReadServerMessage_InitResponse_:
		if m.InitResponse == nil {
			return nil, ErrResponseEmptyInit
		}

		r.session.SessionID = m.InitResponse.SessionId
		r.logger.Log(ctx, log.LevelInfo, "Reader session initialized", map[string]interface{}{
			"sessionID": m.InitResponse.SessionId,
		})
		return &ReaderInit{SessionID: m.InitResponse.SessionId}, nil
	default:
		return nil, fmt.Errorf("persqueue: received unexpected message type %T", m)
	}
}

func PrintIssues(issues []*Ydb_Issue.IssueMessage) string {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("Followed issues: %v\n", len(issues)))
	for _, i := range issues {
		sb.WriteString(fmt.Sprintf("%v\n", i.GetMessage()))
	}
	return sb.String()
}

func (r *readerV1) emit(ctx context.Context, e Event) {
	// Better not to process event for sure if context is canceled.
	if ctx.Err() != nil {
		return
	}

	select {
	case r.c <- e:
	case <-ctx.Done():
	}
}

func (r *readerV1) run(ctx context.Context) {
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
			// Try to keep err == nil only for shutdown.
			if err != nil {
				_ = r.session.Close()
			}
			// Once receiver closed we know that there is no active reads, so reset them
			receiverErrC <- err
			atomic.StoreInt64(&r.activeReads, 0)
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

		if !r.options.RetryOnFailure {
			r.logger.Log(ctx, log.LevelError, "Reader terminated because of error", map[string]interface{}{
				"error": err.Error(),
			})
			r.finish(err)
			return
		}

		r.commitBuffer = newCommitBuffer(r.options.MaxMemory)
		r.lockC = make(chan lockV1Request, lockRequestChannelSize)

		bo := backoff.NewExponentialBackOff()
		for {
			if err := ctx.Err(); err != nil {
				r.logger.Log(ctx, log.LevelInfo, "Reader is cancelled", map[string]interface{}{
					"error": err.Error(),
				})
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
			r.logger.Log(ctx, log.LevelWarn, "Reader session restarted", nil)
			break
		}
	}
}

func (r *readerV1) runSender(ctx context.Context) error {
	limitWaiter := backoff.NewExponentialBackOff()
	limitWaiter.InitialInterval = time.Millisecond
	limitWaiter.MaxElapsedTime = 100 * time.Millisecond

	backoffTimer := backoff.NewExponentialBackOff()
	backoffTimer.Reset()
	backoffTimer.MaxElapsedTime = 0
	nextLogDuration := backoffTimer.NextBackOff()
	logTime := time.Now()

	for {
		select {
		case lock := <-r.lockC:
			if err := r.sendStartRead(lock.lock, lock.verifyReadOffset, lock.readOffset, lock.commitOffset); err != nil {
				return err
			}

		case release := <-r.releaseC:
			if err := r.sendFinishRead(release); err != nil {
				return err
			}

		case <-ctx.Done():
			return ctx.Err()

		default:
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
				limitWaiter.Reset()
				if err := r.sendRead(ctx); err != nil {
					return err
				}
			} else {
				// we need to wait till limits free up
				time.Sleep(limitWaiter.NextBackOff())
			}
		}
	}
}

func (r *readerV1) sendStartRead(lock *LockV1, verifyReadOffset bool, readOffset, commitOffset uint64) error {
	return r.stream.SendMsg(&Ydb_PersQueue_V1.MigrationStreamingReadClientMessage{
		Request: &Ydb_PersQueue_V1.MigrationStreamingReadClientMessage_StartRead_{
			StartRead: &Ydb_PersQueue_V1.MigrationStreamingReadClientMessage_StartRead{
				Topic:            &Ydb_PersQueue_V1.Path{Path: lock.Topic},
				Cluster:          lock.Cluster,
				Partition:        lock.Partition,
				AssignId:         lock.AssignID,
				ReadOffset:       readOffset,
				CommitOffset:     commitOffset,
				VerifyReadOffset: verifyReadOffset,
			},
		},
	})
}

func (r *readerV1) sendFinishRead(release releaseV1Request) error {
	return r.stream.SendMsg(&Ydb_PersQueue_V1.MigrationStreamingReadClientMessage{
		Request: &Ydb_PersQueue_V1.MigrationStreamingReadClientMessage_Released_{
			Released: &Ydb_PersQueue_V1.MigrationStreamingReadClientMessage_Released{
				Topic:     &Ydb_PersQueue_V1.Path{Path: release.Topic},
				Cluster:   release.Cluster,
				Partition: release.Partition,
				AssignId:  release.AssignID,
			},
		},
	})
}

func (r *readerV1) runReceiver(ctx context.Context) error {
	for ctx.Err() == nil {
		rsp := &Ydb_PersQueue_V1.MigrationStreamingReadServerMessage{}
		if err := r.stream.RecvMsg(rsp); err != nil {
			return err
		}
		if rsp.Status != Ydb.StatusIds_SUCCESS {
			return fmt.Errorf("persqueue: status error: %v, Issues: %v", rsp.Status.String(), PrintIssues(rsp.Issues))
		}

		if rsp.Response == nil {
			return ErrResponseIsEmpty
		}

		switch m := rsp.Response.(type) {
		case *Ydb_PersQueue_V1.MigrationStreamingReadServerMessage_DataBatch_:
			atomic.AddInt64(&r.activeReads, -1)

			if m.DataBatch == nil {
				return ErrResponseEmptyBatch
			}

			datas, err := r.handleBatchedData(m.DataBatch)
			if err != nil {
				return err
			}
			for _, data := range datas {
				r.emit(ctx, data)
			}
		case *Ydb_PersQueue_V1.MigrationStreamingReadServerMessage_PartitionStatus_:
			if m.PartitionStatus == nil {
				return ErrResponseEmptyLock
			}

			r.emit(ctx, r.handlePartitionStatus(m.PartitionStatus))
		case *Ydb_PersQueue_V1.MigrationStreamingReadServerMessage_Assigned_:
			if m.Assigned == nil {
				return ErrResponseEmptyLock
			}

			r.emit(ctx, r.handleAssigned(m.Assigned))
		case *Ydb_PersQueue_V1.MigrationStreamingReadServerMessage_Release_:
			if m.Release == nil {
				return ErrResponseEmptyRelease
			}

			r.emit(ctx, r.handleRelease(m.Release))

		case *Ydb_PersQueue_V1.MigrationStreamingReadServerMessage_Committed_:
			if m.Committed == nil {
				return ErrResponseEmptyCommit
			}

			r.emit(ctx, r.handleCommit(m.Committed))

		default:
			return fmt.Errorf("persqueue: received unexpected message %T", m)
		}
	}
	if r.checkShutdown() {
		return nil
	}
	return ctx.Err()
}

func (r *readerV1) sendCommit(ctx context.Context, cookies []PartitionCookie) error {
	commitCookies := make([]*Ydb_PersQueue_V1.CommitCookie, len(cookies))
	for i, c := range cookies {
		commitCookies[i] = &Ydb_PersQueue_V1.CommitCookie{
			AssignId:        c.AssignID,
			PartitionCookie: c.PartitionCookie,
		}
	}
	r.logger.Log(ctx, log.LevelDebug, "Sending commit", map[string]interface{}{
		"cookies": cookies,
	})
	return r.stream.SendMsg(&Ydb_PersQueue_V1.MigrationStreamingReadClientMessage{
		Request: &Ydb_PersQueue_V1.MigrationStreamingReadClientMessage_Commit_{
			Commit: &Ydb_PersQueue_V1.MigrationStreamingReadClientMessage_Commit{
				Cookies: commitCookies,
			},
		},
	})
}

func (r *readerV1) sendRead(ctx context.Context) error {
	r.logger.Log(ctx, log.LevelDebug, "Sending read request", nil)
	return r.stream.SendMsg(&Ydb_PersQueue_V1.MigrationStreamingReadClientMessage{
		Request: &Ydb_PersQueue_V1.MigrationStreamingReadClientMessage_Read_{
			Read: &Ydb_PersQueue_V1.MigrationStreamingReadClientMessage_Read{},
		},
	})
}

func codecFromProtoV1(codec Ydb_PersQueue_V1.Codec) (Codec, error) {
	switch codec {
	case Ydb_PersQueue_V1.Codec_CODEC_RAW, Ydb_PersQueue_V1.Codec_CODEC_UNSPECIFIED:
		return Raw, nil
	case Ydb_PersQueue_V1.Codec_CODEC_GZIP:
		return Gzip, nil
	case Ydb_PersQueue_V1.Codec_CODEC_ZSTD:
		return Zstd, nil
	default:
		return 0, fmt.Errorf("persqueue: unsupported codec %s", codec)
	}
}

func (r *readerV1) handleBatchedData(d *Ydb_PersQueue_V1.MigrationStreamingReadServerMessage_DataBatch) ([]*Data, error) {
	datas := make([]*Data, len(d.PartitionData))
	for pIdx, p := range d.PartitionData {
		var nMessages int
		batches := make([]MessageBatch, len(p.Batches))

		for bIdx, batch := range p.Batches {
			extraFields := convertKeyValue(batch.ExtraFields)
			batchMsgs := make([]ReadMessage, len(batch.MessageData))

			for mIdx, m := range batch.MessageData {
				codec, err := codecFromProtoV1(m.Codec)
				if err != nil {
					return nil, err
				}

				nMessages++
				batchMsgs[mIdx] = ReadMessage{
					Offset: m.Offset,

					SeqNo:    m.SeqNo,
					SourceID: batch.SourceId,

					Data:  m.Data,
					Codec: codec,

					CreateTime: FromTimestamp(m.CreateTimestampMs),
					WriteTime:  FromTimestamp(batch.WriteTimestampMs),
					IP:         batch.Ip,

					ExtraFields: extraFields,
				}

				if err = r.unpack(&batchMsgs[mIdx]); err != nil {
					return nil, err
				}
			}

			batches[bIdx] = MessageBatch{
				Partition: uint32(p.Partition),
				Topic:     p.Topic.Path,
				Messages:  batchMsgs,
			}
		}

		data := &Data{
			batches:      batches,
			commitBuffer: r.commitBuffer,
			Cookie:       p.Cookie.PartitionCookie,
			partitionCookie: PartitionCookie{
				AssignID:        p.Cookie.AssignId,
				PartitionCookie: p.Cookie.PartitionCookie,
			},
		}
		r.commitBuffer.AddCookie(PartitionCookie{AssignID: p.Cookie.AssignId, PartitionCookie: p.Cookie.PartitionCookie}, data.size())
		datas[pIdx] = data
		r.logger.Log(context.Background(), log.LevelDebug, "Received data", map[string]interface{}{
			"nBatches":  len(batches),
			"nMessages": nMessages,
			"cookies":   p.Cookie,
		})
	}

	return datas, nil
}

func (r *readerV1) unpack(m *ReadMessage) (err error) {
	atomic.AddUint64(&r.bytesRead, uint64(len(m.Data)))
	if !r.options.DecompressionDisabled && m.Codec != Raw {
		decPool := getDecPool(m.Codec)
		var dec = decPool.Get().(Decompressor)
		m.Data, err = dec.Decompress(m.Data)
		decPool.Put(dec)
	}
	atomic.AddUint64(&r.bytesExtracted, uint64(len(m.Data)))
	return
}

func convertKeyValue(m []*Ydb_PersQueue_V1.KeyValue) (items map[string]string) {
	if len(m) == 0 {
		return
	}

	items = map[string]string{}
	for _, item := range m {
		items[item.Key] = item.Value
	}
	return
}

func (r *readerV1) handleRelease(m *Ydb_PersQueue_V1.MigrationStreamingReadServerMessage_Release) Event {
	r.logger.Log(context.Background(), log.LevelDebug, "Received release", map[string]interface{}{
		"release": m,
	})

	return &ReleaseV1{
		Generation:   m.AssignId,
		Partition:    m.Partition,
		Topic:        m.Topic.Path,
		Cluster:      m.Cluster,
		CommitOffset: m.CommitOffset,

		releaseC: r.releaseC,
	}
}

func (r *readerV1) handleCommit(m *Ydb_PersQueue_V1.MigrationStreamingReadServerMessage_Committed) Event {
	cookies := make([]uint64, len(m.Cookies))
	partitionCookies := make([]PartitionCookie, len(m.Cookies))

	for i, cookie := range m.Cookies {
		cookies[i] = cookie.PartitionCookie
		partitionCookies[i] = PartitionCookie{AssignID: cookie.AssignId, PartitionCookie: cookie.PartitionCookie}
		r.commitBuffer.AckCommit(PartitionCookie{AssignID: cookie.AssignId, PartitionCookie: cookie.PartitionCookie})
	}
	r.logger.Log(context.Background(), log.LevelDebug, "Received commit ack", map[string]interface{}{
		"cookies": cookies,
	})

	return &CommitAck{Cookies: cookies, PartitionCookies: partitionCookies}
}

func (r *readerV1) handleAssigned(m *Ydb_PersQueue_V1.MigrationStreamingReadServerMessage_Assigned) Event {
	r.logger.Log(context.Background(), log.LevelDebug, "Received assigned", map[string]interface{}{
		"lock": m,
	})

	return &LockV1{
		Topic:      m.Topic.Path,
		Partition:  m.Partition,
		EndOffset:  m.EndOffset,
		AssignID:   m.AssignId,
		Cluster:    m.Cluster,
		ReadOffset: m.ReadOffset,

		lockC: r.lockC,
	}
}

func (r *readerV1) handlePartitionStatus(m *Ydb_PersQueue_V1.MigrationStreamingReadServerMessage_PartitionStatus) Event {
	return &PartitionStatus{
		Cluster:         m.Cluster,
		AssignID:        m.AssignId,
		Partition:       m.Partition,
		Topic:           m.Topic.Path,
		CommittedOffset: m.CommittedOffset,
		EndOffset:       m.EndOffset,
	}
}

// NewReaderV1 creates new reader for topic
// It uses V1 protocol which only protocol allowed for data stream cloud installations
func NewReaderV1(options ReaderOptions) Reader {
	lgr := options.Logger
	if lgr == nil {
		lgr = log.NopLogger
	}

	c := &readerV1{
		logger:   lgr,
		options:  options,
		lockC:    make(chan lockV1Request, lockRequestChannelSize),
		releaseC: make(chan releaseV1Request, lockRequestChannelSize),
		c:        make(chan Event, 1),
		done:     make(chan struct{}),

		commitBuffer: newCommitBuffer(options.MaxMemory),
	}

	return c
}
