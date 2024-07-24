package ydb

import (
	"context"
	"errors"
	"path"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/internal"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/timeutil"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type driver struct {
	cluster *cluster
	meta    *meta
	trace   DriverTrace

	requestTimeout       time.Duration
	streamTimeout        time.Duration
	operationTimeout     time.Duration
	operationCancelAfter time.Duration
}

func (d *driver) Close() error {
	return d.cluster.Close()
}

func isTransportError(err error) bool {
	var te *TransportError
	return errors.As(err, &te)
}

func (d *driver) pessimizeConn(ctx context.Context, conn *conn, cause error) {
	// remove node from discovery cache on any transport error
	driverTracePessimizationDone := driverTraceOnPessimization(ctx, d.trace, ctx, conn.addr.String(), cause)
	driverTracePessimizationDone(ctx, conn.addr.String(), d.cluster.Pessimize(conn.addr))
}

func (d *driver) Call(ctx context.Context, op Operation) (info CallInfo, err error) {
	// Remember raw context to pass it for the tracing functions.
	rawCtx := ctx

	if t := d.requestTimeout; t > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t)
		defer cancel()
	}
	if t := d.operationTimeout; t > 0 {
		ctx = WithOperationTimeout(ctx, t)
	}
	if t := d.operationCancelAfter; t > 0 {
		ctx = WithOperationCancelAfter(ctx, t)
	}

	// Get credentials (token actually) for the request.
	var md metadata.MD
	md, err = d.meta.md(ctx)
	if err != nil {
		return
	}
	if len(md) > 0 {
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	var cc *conn
	cc, err = d.cluster.Get(ctx)
	if err != nil {
		return
	}

	defer func() {
		if err != nil {
			err = mapGRPCError(err)
			if isTransportError(err) {
				d.pessimizeConn(rawCtx, cc, err)
			}
		}
	}()

	var raw *grpc.ClientConn
	raw, err = cc.getConn(ctx)
	if err != nil {
		return
	}

	info = &callInfo{address: cc.Address()}

	method, req, res, resp := internal.Unwrap(op)
	if resp == nil {
		resp = internal.WrapOpResponse(&Ydb_Operations.GetOperationResponse{})
	}

	params := operationParams(ctx)
	if !params.Empty() {
		setOperationParams(req, params)
	}

	onDone := driverTraceOnOperation(ctx, d.trace, ctx, cc.Address(), Method(method), params)
	defer func() {
		onDone(rawCtx, cc.Address(), Method(method), params, resp.GetOpID(), resp.GetIssues(), err)
	}()

	start := timeutil.Now()
	cc.runtime.operationStart(start)
	err = invoke(ctx, raw, resp, method, req, res)
	cc.runtime.operationDone(
		start, timeutil.Now(),
		errIf(isTimeoutError(err), err),
	)

	return
}

func (d *driver) StreamRead(ctx context.Context, op StreamOperation) (info CallInfo, err error) {
	// Remember raw context to pass it for the tracing functions.
	rawCtx := ctx

	var cancel context.CancelFunc
	if t := d.streamTimeout; t > 0 {
		ctx, cancel = context.WithTimeout(ctx, t)
	} else {
		// we want to force cancel goroutine with stream
		ctx, cancel = context.WithCancel(ctx)
	}
	defer func() {
		// if err is nil goroutine not run, and we cancel directly
		if err != nil {
			cancel()
		}
	}()

	// Get credentials (token actually) for the request.
	var md metadata.MD
	md, err = d.meta.md(ctx)
	if err != nil {
		return nil, err
	}
	if len(md) > 0 {
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	var cc *conn
	cc, err = d.cluster.Get(ctx)
	if err != nil {
		return
	}

	defer func() {
		if err != nil {
			err = mapGRPCError(err)
			if isTransportError(err) {
				d.pessimizeConn(rawCtx, cc, err)
			}
		}
	}()

	var raw *grpc.ClientConn
	raw, err = cc.getConn(ctx)
	if err != nil {
		return
	}

	info = &callInfo{address: cc.Address()}

	if err != nil {
		return nil, err
	}

	method, req, resp, process := internal.UnwrapStreamOperation(op)
	desc := grpc.StreamDesc{
		StreamName:    path.Base(method),
		ServerStreams: true,
	}

	cc.runtime.streamStart(timeutil.Now())
	onDone := driverTraceOnStream(ctx, d.trace, ctx, cc.Address(), Method(method))
	defer func() {
		if err != nil {
			cc.runtime.streamDone(timeutil.Now(), err)
			onDone(rawCtx, cc.Address(), Method(method), err)
		}
	}()

	var s grpc.ClientStream
	s, err = grpc.NewClientStream(ctx, &desc, raw, method,
		grpc.MaxCallRecvMsgSize(50*1024*1024), // 50MB
	)
	if err != nil {
		return
	}
	if err = s.SendMsg(req); err != nil {
		return
	}
	if err = s.CloseSend(); err != nil {
		return
	}

	go func() {
		var err error
		defer func() {
			if err != nil {
				if isTransportError(err) {
					d.pessimizeConn(rawCtx, cc, err)
				}
			}
			cc.runtime.streamDone(timeutil.Now(), hideEOF(err))
			onDone(rawCtx, cc.Address(), Method(method), hideEOF(err))
			// cancel directly on exit from goroutine
			// this need for break grpc client stream
			cancel()
		}()
		for ; err == nil; err = func() (err error) { // isolate scope for defer effect without for-defer effect
			onDone := driverTraceOnStreamRecv(ctx, d.trace, ctx, cc.Address(), Method(method))
			defer func() {
				if err == nil {
					cc.runtime.streamRecv(timeutil.Now())
					if s := resp.GetStatus(); s != Ydb.StatusIds_SUCCESS {
						err = &OpError{
							Reason: statusCode(s),
							issues: resp.GetIssues(),
						}
					}
				} else {
					err = mapGRPCError(err)
				}
				// NOTE: do not hide even io.EOF for this call.
				process(err)
				onDone(rawCtx, cc.Address(), Method(method), resp.GetIssues(), hideEOF(err))
			}()
			return s.RecvMsg(resp)
		}() {
		}
	}()

	return
}
