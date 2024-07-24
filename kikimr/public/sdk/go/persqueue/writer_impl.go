package persqueue

import (
	"compress/flate"
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/genproto/Ydb_PersQueue_V0"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/log"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/session"
	"google.golang.org/grpc/status"
)

const (
	ackQueueSize         = 1024
	bufferFlushThreshold = 128
	maxBackoffInterval   = 20 * time.Second
)

var (
	ErrWriterIsClosed                  = errors.New("persqueue: writer is closed")
	ErrWriterReceivedUnknownMessageAck = errors.New("persqueue: received ack for unknown message")
	ErrWriterEmptyAck                  = errors.New("persqueue: ack is empty")
	ErrWriterEmptyBatchAck             = errors.New("persqueue: batch ack is empty")
)

type writer struct {
	options WriterOptions
	logger  log.Logger

	inflight inflight

	push       chan *WriteMessage
	compressed <-chan WriteMessageOrError
	ack        chan WriteResponse

	nextSeqNo uint64
	sessionID string

	lock      sync.RWMutex
	closed    chan struct{}
	err       error
	muSession *session.Session
	muStream  Ydb_PersQueue_V0.PersQueueService_WriteSessionClient
}

func (w *writer) sessionOptions() session.Options {
	return session.Options{
		Endpoint:             w.options.Endpoint,
		Port:                 w.options.Port,
		Credentials:          w.options.Credentials,
		TLSConfig:            w.options.TLSConfig,
		Logger:               w.options.Logger,
		Database:             w.options.Database,
		ClientTimeout:        w.options.ClientTimeout,
		DiscoverCluster:      w.options.DiscoverCluster,
		Topic:                w.options.Topic,
		SourceID:             w.options.SourceID,
		PartitionGroup:       w.options.PartitionGroup,
		PreferredClusterName: w.options.PreferredClusterName,
	}.WithProxy(w.options.Proxy())
}

func (w *writer) Write(msg *WriteMessage) error {
	select {
	case w.push <- msg:
		return nil

	case <-w.closed:
		if w.err != nil {
			return w.err
		} else {
			return ErrWriterIsClosed
		}
	}
}

func (w *writer) Close() error {
	select {
	case <-w.closed:
		return w.err

	default:
		close(w.push)

		<-w.closed
		return w.err
	}
}

func (w *writer) Closed() <-chan struct{} {
	return w.closed
}

func (w *writer) Stat() WriterStat {
	return WriterStat{
		MemUsage: w.inflight.Size(),
		Inflight: w.inflight.Length(),
	}
}

func (w *writer) Init(ctx context.Context) (*WriterInit, error) {
	for {
		init, err := w.init(ctx)
		if err != nil {
			if nerr, ok := err.(temparable); err != context.DeadlineExceeded && ok && nerr.Temporary() && w.options.RetryOnFailure {
				w.logger.Log(ctx, log.LevelError, "Retry start error", map[string]interface{}{
					"error": err.Error(),
				})
				continue
			}

			w.err = err
			close(w.closed)
			close(w.ack)
			close(w.push)
			return nil, err
		}

		go w.run(ctx, init)
		return init, nil
	}
}

func (w *writer) init(ctx context.Context) (init *WriterInit, err error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.muSession != nil {
		if closeErr := w.muSession.Close(); closeErr != nil {
			w.logger.Log(ctx, log.LevelWarn, "Session close error before init", map[string]interface{}{
				"error": closeErr.Error(),
			})
		}
	}

	w.muSession, err = session.Dial(ctx, w.sessionOptions())
	if err != nil {
		if statusErr, ok := status.FromError(err); ok {
			return nil, &GRPCError{Status: statusErr.Code(), Description: statusErr.Message()}
		}
		return
	}

	defer func() {
		if err != nil {
			if closeErr := w.muSession.Close(); closeErr != nil {
				w.logger.Log(ctx, log.LevelWarn, "Session close error", map[string]interface{}{
					"error": closeErr.Error(),
				})
			}
			w.muSession = nil
		}
	}()

	InternalSessionHook(w.muSession)

	w.muStream, err = w.muSession.Client.WriteSession(ctx)
	if err != nil {
		if statusErr, ok := status.FromError(err); ok {
			return nil, &GRPCError{Status: statusErr.Code(), Description: statusErr.Message()}
		}
		return nil, err
	}

	message := &Ydb_PersQueue_V0.WriteRequest_Init{
		Topic:          w.options.Topic,
		SourceId:       w.options.SourceID,
		ExtraFields:    mapToLBMap(w.options.ExtraAttrs),
		ProxyCookie:    w.muSession.Cookie,
		PartitionGroup: w.options.PartitionGroup,
		Version:        Version,
	}

	err = w.muStream.SendMsg(
		&Ydb_PersQueue_V0.WriteRequest{
			Request:     &Ydb_PersQueue_V0.WriteRequest_Init_{Init: message},
			Credentials: w.muSession.GetCredentials(ctx),
		})

	if err != nil {
		if statusErr, ok := status.FromError(err); ok {
			return nil, &GRPCError{Status: statusErr.Code(), Description: statusErr.Message()}
		}
		return nil, err
	}

	wrapper := &Ydb_PersQueue_V0.WriteResponse{}
	if err = w.muStream.RecvMsg(wrapper); err != nil {
		if statusErr, ok := status.FromError(err); ok {
			return nil, &GRPCError{Status: statusErr.Code(), Description: statusErr.Message()}
		}
		return nil, err
	}

	switch msg := wrapper.Response.(type) {
	case *Ydb_PersQueue_V0.WriteResponse_Error:
		if msg.Error == nil {
			return nil, ErrResponseEmptyError
		}

		return nil, &Error{Code: int(msg.Error.Code), Description: msg.Error.Description}

	case *Ydb_PersQueue_V0.WriteResponse_Init_:
		if msg.Init == nil {
			return nil, ErrResponseEmptyInit
		}

		reply := msg.Init

		w.nextSeqNo = reply.MaxSeqNo + 1
		w.sessionID = reply.SessionId

		init = &WriterInit{
			MaxSeqNo:  reply.GetMaxSeqNo(),
			SessionID: reply.GetSessionId(),
			Topic:     reply.GetTopic(),
			Partition: uint64(reply.GetPartition()),
		}

		w.logger.Log(ctx, log.LevelDebug, "Writer initialized", map[string]interface{}{
			"init": init,
		})
		return

	case nil:
		return nil, ErrResponseEmptyInitReply

	default:
		return nil, fmt.Errorf("persqueue: received unexpected message type %T", msg)
	}
}

func (w *writer) finish(ctx context.Context, err error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	select {
	case <-w.closed:
		return
	default:
		if err != nil {
			w.logger.Log(ctx, log.LevelError, "Writer terminated with error", map[string]interface{}{
				"error": err.Error(),
			})
		} else {
			w.logger.Log(ctx, log.LevelInfo, "Writer terminated", nil)
		}

		w.err = err
		close(w.closed)
	}
}

func (w *writer) sendWriteResponse(ctx context.Context, response WriteResponse) error {
	select {
	case w.ack <- response:
		return nil
	case <-ctx.Done():
		return context.Canceled
	}
}

func (w *writer) sendIssues(ctx context.Context) {
	for {
		msg := w.inflight.Pop()
		if msg == nil {
			break
		}

		if err := w.sendWriteResponse(ctx, &Issue{
			Data: msg,
			Err:  w.err,
		}); err != nil {
			break
		}
	}

	close(w.ack)
}

func (w *writer) run(ctx context.Context, init *WriterInit) {
	// Keeping writer alive is responsibility of this goroutine.
	defer w.sendIssues(ctx)
	defer w.finish(ctx, nil)

	for {
		receiverErr := make(chan error, 1)
		senderErr := make(chan error, 1)

		session, stream := w.getSessionAndStream()

		go func() {
			err := w.runReceiver(ctx, stream)

			_ = session.Close()
			receiverErr <- err
		}()

		go func() {
			if err := w.resend(ctx, init); err != nil {
				senderErr <- err
				_ = session.Close()
				return
			}

			err := w.runSender(ctx)
			if err != nil {
				_ = session.Close()
			}
			senderErr <- err
		}()

		var err error
		select {
		case err = <-receiverErr:
			// Receiver crashed. Wait for sender exit and restart.
			<-senderErr
		case err = <-senderErr:
			if err == nil {
				// Client closed the writer. Need to wait for acks on all inflight messages and then exit.

			waitAck:
				for {
					select {
					case err = <-receiverErr:
						break waitAck
					default:
					}

					if w.inflight.Size() == 0 {
						_ = session.Close()
						<-receiverErr
						return
					}

					time.Sleep(time.Millisecond * 100)
				}
			} else {
				// Sender crashed. Wait for receiver exit and restart.
				<-receiverErr
			}
		}

		if w.options.RetryOnFailure {
			ticker := getReconnectTicker()
			for range ticker.C {
				if err := ctx.Err(); err != nil {
					// Context is canceled. There is no point in trying to restart.
					w.finish(ctx, err)
					ticker.Stop()
					return
				}

				w.logger.Log(ctx, log.LevelWarn, "Restarting writer because of error", map[string]interface{}{
					"error": err.Error(),
				})

				init, err = w.init(ctx)
				if err == nil {
					ticker.Stop()
					break
				}
			}
		} else {
			w.finish(ctx, err)
			return
		}
	}
}

func (w *writer) resend(ctx context.Context, init *WriterInit) error {
	for {
		msg := w.inflight.Peek()
		if msg == nil {
			return nil
		}

		if msg.assignedSeqNo <= init.MaxSeqNo {
			w.inflight.Pop()

			if err := w.sendWriteResponse(ctx, &Ack{
				SeqNo:          msg.assignedSeqNo,
				AlreadyWritten: true,
			}); err != nil {
				return err
			}
		} else {
			break
		}
	}

	resend := w.inflight.PeekAll()

	for i := 0; i < len(resend); i += bufferFlushThreshold {
		size := bufferFlushThreshold
		if i+size > len(resend) {
			size = len(resend) - i
		}

		buffer := resend[i : i+size]
		w.logger.Log(ctx, log.LevelWarn, "Resending inflight messages: minSeqNo=%d, batchSize=%d", map[string]interface{}{
			"minSeqNo":  buffer[0].assignedSeqNo,
			"batchSize": len(buffer),
		})
		if err := w.sendBatchMessage(ctx, buffer); err != nil {
			return err
		}
	}

	if len(resend) > 0 {
		w.nextSeqNo = resend[len(resend)-1].assignedSeqNo + 1
	}

	return nil
}

func (w *writer) runSender(ctx context.Context) error {
	flushTick := time.NewTicker(time.Millisecond * 100)
	defer flushTick.Stop()

	compressed := w.compressed
	checkMaxMemory := func() {
		if w.options.MaxMemory != 0 {
			if w.inflight.Size() > w.options.MaxMemory {
				compressed = nil
			} else {
				compressed = w.compressed
			}
		}
	}

	var buffer []*WriteMessage
	flushBuffer := func() error {
		err := w.sendBatchMessage(ctx, buffer)
		buffer = buffer[:0]
		return err
	}

	for {
		select {
		case msg, ok := <-compressed:
			if !ok {
				// Client closed writer. Flush buffer and terminate.
				return flushBuffer()
			}

			if msg.Err != nil {
				// Compression error should be impossible. Treat this type of error as fatal, just in case.
				w.finish(ctx, msg.Err)
				return msg.Err
			}

			m := msg.Msg
			if m.assignedSeqNo == 0 {
				m.assignedSeqNo = w.nextSeqNo
				w.nextSeqNo++
			} else {
				w.nextSeqNo = m.assignedSeqNo + 1
			}

			w.inflight.Push(msg.Msg)
			buffer = append(buffer, msg.Msg)
			checkMaxMemory()

		case <-flushTick.C:
			checkMaxMemory()

			if err := flushBuffer(); err != nil {
				return err
			}

		case <-ctx.Done():
			return ctx.Err()
		}

		if len(buffer) > bufferFlushThreshold {
			if err := flushBuffer(); err != nil {
				return err
			}
		}
	}
}

func (w *writer) onAck(ctx context.Context, ack *Ydb_PersQueue_V0.WriteResponse_Ack) (err error) {
	w.logger.Log(ctx, log.LevelDebug, "Received ack", map[string]interface{}{
		"ack": ack,
	})

	msg := w.inflight.Pop()
	if msg == nil {
		err = ErrWriterReceivedUnknownMessageAck
		// Something is really wrong with the protocol. There is no point in trying to reconnect.
		w.finish(ctx, err)
		return
	}

	if msg.assignedSeqNo != ack.SeqNo {
		err = fmt.Errorf("persqueue: seq order violated: %d != %d", msg.assignedSeqNo, ack.SeqNo)
		// Something is really wrong with the protocol. There is no point in trying to reconnect.
		w.finish(ctx, err)
		return
	}

	return w.sendWriteResponse(ctx, &Ack{
		Offset:         ack.GetOffset(),
		SeqNo:          ack.GetSeqNo(),
		AlreadyWritten: ack.GetAlreadyWritten(),
	})
}

func (w *writer) runReceiver(ctx context.Context, stream Ydb_PersQueue_V0.PersQueueService_WriteSessionClient) (err error) {
	for {
		msg := &Ydb_PersQueue_V0.WriteResponse{}
		if err := stream.RecvMsg(msg); err != nil {
			return err
		}

		if msg.Response == nil {
			return ErrResponseIsEmpty
		}

		w.logger.Log(ctx, log.LevelDebug, "Received msg", map[string]interface{}{
			"msg": msg.Response,
		})
		switch rsp := msg.Response.(type) {
		case *Ydb_PersQueue_V0.WriteResponse_Ack_:
			if rsp.Ack == nil {
				return ErrWriterEmptyAck
			}

			if err := w.onAck(ctx, rsp.Ack); err != nil {
				return err
			}

		case *Ydb_PersQueue_V0.WriteResponse_AckBatch_:
			if rsp.AckBatch == nil {
				return ErrWriterEmptyBatchAck
			}

			for _, ack := range rsp.AckBatch.Ack {
				if err := w.onAck(ctx, ack); err != nil {
					return err
				}
			}

		case *Ydb_PersQueue_V0.WriteResponse_Error:
			if rsp.Error == nil {
				return ErrResponseEmptyError
			}

			return &Error{int(rsp.Error.Code), rsp.Error.Description}

		case nil:
			return ErrResponseIsEmpty

		default:
			return fmt.Errorf("persqueue: received unexpected message %T", rsp)
		}
	}
}

func (w *writer) newWriteRequest(data *WriteMessage) *Ydb_PersQueue_V0.WriteRequest_Data {
	d := &Ydb_PersQueue_V0.WriteRequest_Data{
		SeqNo: data.assignedSeqNo,
		Data:  data.Data,
		Codec: Ydb_PersQueue_V0.ECodec(w.options.Codec),
	}

	if data.CreateTimestamp.IsZero() {
		d.CreateTimeMs = ToTimestamp(time.Now())
	} else {
		d.CreateTimeMs = ToTimestamp(data.CreateTimestamp)
	}

	return d
}

func (w *writer) getSessionAndStream() (*session.Session, Ydb_PersQueue_V0.PersQueueService_WriteSessionClient) {
	w.lock.RLock()
	defer w.lock.RUnlock()

	return w.muSession, w.muStream
}

func (w *writer) sendBatchMessage(ctx context.Context, data []*WriteMessage) error {
	session, stream := w.getSessionAndStream()

	if err := stream.Context().Err(); err != nil {
		return err
	}

	if len(data) == 0 {
		return nil
	}

	var ds []*Ydb_PersQueue_V0.WriteRequest_Data
	for _, d := range data {
		ds = append(ds, w.newWriteRequest(d))
	}
	message := &Ydb_PersQueue_V0.WriteRequest_DataBatch{Data: ds}

	w.logger.Log(ctx, log.LevelDebug, "Sending batch", map[string]interface{}{
		"minSeqNo": data[0].assignedSeqNo,
		"maxSeqNo": data[len(data)-1].assignedSeqNo,
	})

	return stream.SendMsg(
		&Ydb_PersQueue_V0.WriteRequest{
			Request:     &Ydb_PersQueue_V0.WriteRequest_DataBatch_{DataBatch: message},
			Credentials: session.GetCredentials(ctx),
		})
}

func (w *writer) C() <-chan WriteResponse {
	return w.ack
}

func mapToLBMap(m map[string]string) *Ydb_PersQueue_V0.MapType {
	var items []*Ydb_PersQueue_V0.KeyValue
	for k, v := range m {
		items = append(items, &Ydb_PersQueue_V0.KeyValue{Key: k, Value: v})
	}
	return &Ydb_PersQueue_V0.MapType{Items: items}
}

func getReconnectTicker() *backoff.Ticker {
	opts := backoff.NewExponentialBackOff()
	opts.MaxElapsedTime = 0
	opts.MaxInterval = maxBackoffInterval
	ticker := backoff.NewTicker(opts)
	return ticker
}

// NewWriter creates new writer for topic
func NewWriter(options WriterOptions) Writer {
	var lgr log.Logger
	if options.Logger != nil {
		lgr = options.Logger
	} else {
		options.Logger = log.NopLogger
		lgr = log.NopLogger
	}

	if options.CompressionLevel == 0 {
		if options.Codec == Zstd {
			options.CompressionLevel = zstdBestCompression
		}
		if options.Codec == Gzip {
			options.CompressionLevel = flate.BestCompression
		}
	}

	p := writer{
		options: options,
		closed:  make(chan struct{}),
		push:    make(chan *WriteMessage),
		ack:     make(chan WriteResponse, ackQueueSize),
		logger:  lgr,
	}

	compressionConcurrency := p.options.CompressionConcurrency
	if compressionConcurrency <= 0 {
		compressionConcurrency = runtime.NumCPU() - 1
	}

	if p.options.Codec == Raw {
		p.compressed = compressRaw(p.push, p.closed, compressionConcurrency)
	} else {
		compressorPool := getCompressorPool(p.options)
		p.compressed = compressWithPool(p.push, p.closed, compressorPool, compressionConcurrency)
	}
	return &p
}

func getCompressorPool(options WriterOptions) CompressorPool {
	if cpp := options.CompressorPoolProvider; cpp != nil {
		if cp := cpp.Get(options.Codec, options.CompressionLevel); cp != nil {
			return cp
		}
	}
	return NewCompressorPool(options.Codec, options.CompressionLevel)
}
