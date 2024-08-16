/*
Package ydbtest provides tools for stubbing ydb server up.
*/package ydbtest

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Discovery"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"google.golang.org/grpc"
)

type Handler func(ctx context.Context, req RequestParser) (res interface{}, err error)

type Handlers map[ydb.Method]Handler

type RequestParser func(interface{})
type ResponseMapper func(RequestParser) proto.Message

func Ident(res proto.Message) ResponseMapper {
	return func(RequestParser) proto.Message {
		return res
	}
}

func operationResponse(r proto.Message, id string, status Ydb.StatusIds_StatusCode) interface{} {
	res, err := proto.Marshal(r)
	if err != nil {
		panic(err)
	}
	return &Ydb_Operations.GetOperationResponse{
		Operation: &Ydb_Operations.Operation{
			Id:     id,
			Ready:  true,
			Status: status,
			Result: &any.Any{
				Value: res,
			},
		},
	}
}

func SuccessHandler(f ResponseMapper) Handler {
	var id int32
	return func(_ context.Context, req RequestParser) (_ interface{}, err error) {
		x := atomic.AddInt32(&id, 1)
		r := f(req)
		return operationResponse(
			r, strconv.Itoa(int(x)),
			Ydb.StatusIds_SUCCESS,
		), nil
	}
}

type YDB struct {
	Database string
	Handlers Handlers
	T        testing.TB

	once  sync.Once
	descs []grpc.ServiceDesc

	mu        sync.Mutex
	endpoints map[ydb.Endpoint]*Endpoint
}

func (s *YDB) init() {
	s.once.Do(func() {
		m := make(map[string][]grpc.MethodDesc)
		for x, handler := range s.Handlers {
			service := x.Service()
			m[service] = append(m[service], grpc.MethodDesc{
				MethodName: x.Name(),
				Handler: func(
					_ interface{}, ctx context.Context,
					dec func(interface{}) error, _ grpc.UnaryServerInterceptor,
				) (interface{}, error) {
					return handler(ctx, func(x interface{}) {
						if err := dec(x); err != nil {
							s.T.Fatalf("decode request error: %v", err)
						}
					})
				},
			})
		}
		for service, methods := range m {
			s.descs = append(s.descs, grpc.ServiceDesc{
				ServiceName: service,
				Methods:     methods,
				HandlerType: (*stubHandler)(nil),
			})
		}

		s.endpoints = make(map[ydb.Endpoint]*Endpoint)
	})
}

type Endpoint struct {
	db     *YDB
	ln     *Listener
	id     ydb.Endpoint
	server *grpc.Server
}

// StartEndpoint starts to provide a YDB service on some local address.
func (s *YDB) StartEndpoint() *Endpoint {
	s.init()

	s.mu.Lock()
	defer s.mu.Unlock()

	ln := NewListener()
	host, port, err := ln.HostPort()
	if err != nil {
		s.T.Fatal(err)
	}
	e := ydb.Endpoint{
		Addr: host,
		Port: port,
	}
	srv := grpc.NewServer()
	for _, desc := range s.descs {
		srv.RegisterService(&desc, stubHandler(1))
	}
	go func() {
		if err := srv.Serve(ln); err != nil {
			s.T.Fatal(err)
		}
	}()

	x := &Endpoint{
		ln:     ln,
		db:     s,
		id:     e,
		server: srv,
	}
	s.endpoints[e] = x

	return x
}

func (e *Endpoint) Close() {
	e.db.mu.Lock()
	defer e.db.mu.Unlock()

	_, has := e.db.endpoints[e.id]
	if !has {
		e.db.T.Fatalf("endpoint not exists: %v", e.id)
	}
	delete(e.db.endpoints, e.id)
	e.server.Stop()
	_ = e.ln.Close()
}

func (e *Endpoint) DialContext(ctx context.Context) (net.Conn, error) {
	return e.ln.DialContext(ctx)
}

func (e *Endpoint) ServerConn() <-chan net.Conn {
	return e.ln.Server
}

type Balancer struct {
	ln     *Listener
	server *grpc.Server
}

func (b *Balancer) Addr() net.Addr {
	return b.ln.Addr()
}

func (b *Balancer) Close() error {
	b.server.Stop()
	return b.ln.Close()
}

func (b *Balancer) DialContext(ctx context.Context) (net.Conn, error) {
	return b.ln.DialContext(ctx)
}

// StartBalancer starts to provide a discovery service on some local address.
func (s *YDB) StartBalancer() *Balancer {
	s.init()

	ln := NewListener()
	srv := grpc.NewServer()
	m := ydb.Method("/Ydb.Discovery.V1.DiscoveryService/ListEndpoints")

	service, method := m.Split()
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: service,
		Methods: []grpc.MethodDesc{
			{
				MethodName: method,
				Handler: func(
					_ interface{}, ctx context.Context,
					dec func(interface{}) error, _ grpc.UnaryServerInterceptor,
				) (interface{}, error) {
					req := new(Ydb_Discovery.ListEndpointsRequest)
					if err := dec(req); err != nil {
						return nil, err
					}
					res, err := s.listEndpoints(req.Database)
					if err != nil {
						return nil, err
					}
					return operationResponse(
						res, "0", Ydb.StatusIds_SUCCESS,
					), nil
				},
			},
		},
		HandlerType: (*stubHandler)(nil),
	}, stubHandler(1))
	go func() {
		if err := srv.Serve(ln); err != nil {
			s.T.Fatal(err)
		}
	}()

	return &Balancer{
		ln:     ln,
		server: srv,
	}
}

func (s *YDB) DialContext(ctx context.Context, addr string) (_ net.Conn, err error) {
	var e ydb.Endpoint
	e.Addr, e.Port, err = SplitHostPort(addr)
	if err != nil {
		return
	}
	s.mu.Lock()
	x := s.endpoints[e]
	s.mu.Unlock()
	if x == nil {
		return nil, fmt.Errorf("no such endpoint: %v", e)
	}
	return x.DialContext(ctx)
}

func (s *YDB) listEndpoints(db string) (resp *Ydb_Discovery.ListEndpointsResult, err error) {
	if db != s.Database {
		s.T.Errorf(
			"discovery request for %q database; have %q",
			db, s.Database,
		)
		return nil, fmt.Errorf("ydbtest: unknown database: %q", db)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	es := make([]*Ydb_Discovery.EndpointInfo, 0, len(s.endpoints))
	for e := range s.endpoints {
		es = append(es, &Ydb_Discovery.EndpointInfo{
			Address:    e.Addr,
			Port:       uint32(e.Port),
			LoadFactor: e.LoadFactor,
		})
	}
	return &Ydb_Discovery.ListEndpointsResult{
		Endpoints: es,
	}, nil
}

type stubHandler interface{}

type Listener struct {
	Client chan net.Conn
	Server chan net.Conn
	Closed chan struct{}

	closeOnce sync.Once
}

func NewListener() *Listener {
	return &Listener{
		Client: make(chan net.Conn),
		Server: make(chan net.Conn, 1),
		Closed: make(chan struct{}),
	}
}

func (ln *Listener) HostPort() (host string, port int, err error) {
	return SplitHostPort(ln.Addr().String())
}

func SplitHostPort(addr string) (host string, port int, err error) {
	var prt string
	host, prt, err = net.SplitHostPort(addr)
	if err != nil {
		return
	}
	port, err = strconv.Atoi(prt)
	return
}

func (ln *Listener) Accept() (net.Conn, error) {
	s, c := net.Pipe()
	select {
	case ln.Client <- c:
	case <-ln.Closed:
		return nil, fmt.Errorf("closed")
	}
	select {
	case ln.Server <- s:
	default:
	}
	return s, nil
}

func (ln *Listener) Addr() net.Addr {
	return addr(fmt.Sprintf("local:%d", uint32(uintptr(unsafe.Pointer(ln)))))
}

type addr string

func (a addr) Network() string { return "mem" }
func (a addr) String() string  { return string(a) }

func (ln *Listener) Close() error {
	ln.closeOnce.Do(func() {
		close(ln.Closed)
	})
	return nil
}

func (ln *Listener) DialContext(ctx context.Context) (net.Conn, error) {
	select {
	case c := <-ln.Client:
		return c, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (ln *Listener) DialGRPC() (*grpc.ClientConn, error) {
	return grpc.Dial("",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return ln.DialContext(ctx)
		}),
		grpc.WithInsecure(),
	)
}
