package ydb

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

func TestClusterFastRedial(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ln := newStubListener()
	srv := grpc.NewServer()
	go func() {
		_ = srv.Serve(ln)
	}()

	cs, balancer := simpleBalancer()
	c := &cluster{
		dial: func(ctx context.Context, s string) (*grpc.ClientConn, error) {
			return ln.Dial(ctx)
		},
		balancer: balancer,
		index:    make(map[connAddr]connEntry),
	}

	pingConnects := func(size int) chan struct{} {
		done := make(chan struct{})
		go func() {
			for i := 0; i < size*10; i++ {
				cc, err := c.Get(context.Background())
				// enforce close bad connects to track them
				if err == nil && cc != nil && cc.addr.addr == "bad" {
					_ = cc.close()
				}
			}
			close(done)
		}()
		return done
	}

	ne := []Endpoint{
		{Addr: "foo"},
		{Addr: "bad"},
	}
	mergeEndpointIntoCluster(ctx, c, []Endpoint{}, ne)
	select {
	case <-pingConnects(len(ne)):

	case <-time.After(time.Second * 10):
		t.Fatalf("Time limit exceeded while %d endpoints in balance. Wait channel used", len(*cs))
	}
}

func TestClusterMergeEndpoints(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ln := newStubListener()
	srv := grpc.NewServer()
	go func() {
		_ = srv.Serve(ln)
	}()

	_, balancer := simpleBalancer()
	c := &cluster{
		dial: func(ctx context.Context, s string) (*grpc.ClientConn, error) {
			return ln.Dial(ctx)
		},
		balancer: balancer,
		index:    make(map[connAddr]connEntry),
	}

	pingConnects := func(size int) {
		for i := 0; i < size*10; i++ {
			sub, cancel := context.WithTimeout(ctx, time.Millisecond)
			defer cancel()
			cc, err := c.Get(sub)
			// enforce close bad connects to track them
			if err == nil && cc != nil && cc.addr.addr == "bad" {
				_ = cc.close()
			}
		}
	}
	assert := func(t *testing.T, exp []Endpoint) {
		if len(c.index) != len(exp) {
			t.Fatalf("unexpected number of endpoints %d: got %d", len(exp), len(c.index))
		}
		// check all endpoints in exp exists in c.index
		for _, e := range exp {
			if _, ok := c.index[connAddr{addr: e.Addr, port: e.Port}]; !ok {
				t.Fatalf("not found endpoint '%v' in index", e)
			}
		}
		// check all endpoints in c.index exists in exp
		for _, entry := range c.index {
			if !(func() bool { // check entry exists in exp
				for _, e := range exp {
					if e.Addr == entry.conn.addr.addr && e.Port == entry.conn.addr.port {
						// entry exists in exp
						return true
					}
				}
				// entry not exists in exp
				return false
			}()) {
				t.Fatalf("unexpected endpoint '%v' in index", entry.conn.Address())
			}
		}
	}

	endpoints := []Endpoint{
		{Addr: "foo"},
		{Addr: "foo", Port: 123},
	}
	badEndpoints := []Endpoint{
		{Addr: "bad"},
		{Addr: "bad", Port: 123},
	}
	nextEndpoints := []Endpoint{
		{Addr: "foo"},
		{Addr: "bar"},
		{Addr: "bar", Port: 123},
	}
	nextBadEndpoints := []Endpoint{
		{Addr: "bad", Port: 23},
	}
	t.Run("initial fill", func(t *testing.T) {
		ne := append(endpoints, badEndpoints...)
		// merge new endpoints into balancer
		mergeEndpointIntoCluster(ctx, c, []Endpoint{}, ne)
		// try endpoints, filter out bad ones to tracking
		pingConnects(len(ne))
		assert(t, ne)
	})
	t.Run("update with another endpoints", func(t *testing.T) {
		ne := append(nextEndpoints, nextBadEndpoints...)
		// merge new endpoints into balancer
		mergeEndpointIntoCluster(ctx, c, append(endpoints, badEndpoints...), ne)
		// try endpoints, filter out bad ones to tracking
		pingConnects(len(ne))
		assert(t, ne)
	})
	t.Run("left only bad", func(t *testing.T) {
		ne := nextBadEndpoints
		// merge new endpoints into balancer
		mergeEndpointIntoCluster(ctx, c, append(nextEndpoints, nextBadEndpoints...), ne)
		// try endpoints, filter out bad ones to tracking
		pingConnects(len(ne))
		assert(t, ne)
	})
	t.Run("left only good", func(t *testing.T) {
		ne := nextEndpoints
		// merge new endpoints into balancer
		mergeEndpointIntoCluster(ctx, c, nextBadEndpoints, ne)
		// try endpoints, filter out bad ones to tracking
		pingConnects(len(ne))
		assert(t, ne)
	})
}

func TestClusterAwait(t *testing.T) {
	const timeout = 100 * time.Millisecond

	ln := newStubListener()
	srv := grpc.NewServer()
	go func() {
		_ = srv.Serve(ln)
	}()

	var connToReturn *conn
	c := &cluster{
		dial: func(ctx context.Context, _ string) (_ *grpc.ClientConn, err error) {
			return ln.Dial(ctx)
		},
		balancer: stubBalancer{
			OnInsert: func(c *conn, _ connInfo) balancerElement {
				connToReturn = c
				return c.addr
			},
			OnNext: func() *conn {
				return connToReturn
			},
		},
		index: make(map[connAddr]connEntry),
	}
	get := func() (<-chan error, context.CancelFunc) {
		ctx, cancel := context.WithCancel(context.Background())
		got := make(chan error)
		go func() {
			_, err := c.Get(ctx)
			got <- err
		}()
		return got, cancel
	}
	{
		got, cancel := get()
		cancel()
		assertRecvError(t, timeout, got, context.Canceled)
	}
	{
		got, cancel := get()
		defer cancel()
		assertRecvError(t, timeout, got, ErrClusterEmpty)
	}
	{
		c.Insert(context.Background(), Endpoint{})
		got, cancel := get()
		defer cancel()
		assertRecvError(t, timeout, got, nil)
	}
}

type stubBalancer struct {
	OnNext      func() *conn
	OnInsert    func(*conn, connInfo) balancerElement
	OnUpdate    func(balancerElement, connInfo)
	OnRemove    func(balancerElement)
	OnPessimize func(balancerElement) error
	OnContains  func(balancerElement) bool
}

func simpleBalancer() (*connList, balancer) {
	cs := new(connList)
	var i int
	return cs, stubBalancer{
		OnNext: func() *conn {
			n := len(*cs)
			if n == 0 {
				return nil
			}
			e := (*cs)[i%n]
			i++
			return e.conn
		},
		OnInsert: func(conn *conn, info connInfo) balancerElement {
			return cs.Insert(conn, info)
		},
		OnRemove: func(x balancerElement) {
			e := x.(*connListElement)
			cs.Remove(e)
		},
		OnUpdate: func(x balancerElement, info connInfo) {
			e := x.(*connListElement)
			e.info = info
		},
		OnPessimize: func(x balancerElement) error {
			e := x.(*connListElement)
			e.conn.runtime.setState(ConnBanned)
			return nil
		},
		OnContains: func(x balancerElement) bool {
			e := x.(*connListElement)
			return cs.Contains(e)
		},
	}
}

func (s stubBalancer) Next() *conn {
	if f := s.OnNext; f != nil {
		return f()
	}
	return nil
}
func (s stubBalancer) Insert(c *conn, i connInfo) balancerElement {
	if f := s.OnInsert; f != nil {
		return f(c, i)
	}
	return nil
}
func (s stubBalancer) Update(el balancerElement, i connInfo) {
	if f := s.OnUpdate; f != nil {
		f(el, i)
	}
}
func (s stubBalancer) Remove(el balancerElement) {
	if f := s.OnRemove; f != nil {
		f(el)
	}
}
func (s stubBalancer) Pessimize(el balancerElement) error {
	if f := s.OnPessimize; f != nil {
		return f(el)
	}
	return nil
}

func (s stubBalancer) Contains(el balancerElement) bool {
	if f := s.OnContains; f != nil {
		return f(el)
	}
	return false
}

type stubListener struct {
	C chan net.Conn // Client half of the connection.
	S chan net.Conn // Server half of the connection.

	once sync.Once
	exit chan struct{}
}

func newStubListener() *stubListener {
	return &stubListener{
		C: make(chan net.Conn),
		S: make(chan net.Conn, 1),

		exit: make(chan struct{}),
	}
}

func (ln *stubListener) Accept() (net.Conn, error) {
	s, c := net.Pipe()
	select {
	case ln.C <- c:
	case <-ln.exit:
		return nil, fmt.Errorf("closed")
	}
	select {
	case ln.S <- s:
	default:
	}
	return s, nil
}

func (ln *stubListener) Addr() net.Addr {
	return &net.TCPAddr{}
}

func (ln *stubListener) Close() error {
	ln.once.Do(func() {
		close(ln.exit)
	})
	return nil
}

func (ln *stubListener) Dial(ctx context.Context) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, "",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			select {
			case <-ln.exit:
				return nil, fmt.Errorf("refused")
			case c := <-ln.C:
				return c, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}),
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    time.Second,
			Timeout: time.Second,
		}),
	)
}

func assertRecvError(t *testing.T, d time.Duration, e <-chan error, exp error) {
	select {
	case act := <-e:
		if act != exp {
			t.Errorf("%s: unexpected error: %v; want %v", fileLine(2), act, exp)
		}
	case <-time.After(d):
		t.Errorf("%s: nothing received after %s", fileLine(2), d)
	}
}

func mergeEndpointIntoCluster(ctx context.Context, c *cluster, curr, next []Endpoint) {
	sortEndpoints(curr)
	sortEndpoints(next)
	diffEndpoints(curr, next,
		func(i, j int) { c.Update(ctx, next[j]) },
		func(i, j int) { c.Insert(ctx, next[j]) },
		func(i, j int) { c.Remove(ctx, curr[i]) },
	)
}

func TestDiffEndpoint(t *testing.T) {
	// lists must be sorted
	noEndpoints := []Endpoint{}
	someEndpoints := []Endpoint{
		{
			Addr: "0",
			Port: 0,
		},
		{
			Addr: "1",
			Port: 1,
		},
	}
	sameSomeEndpoints := []Endpoint{
		{
			Addr:       "0",
			Port:       0,
			LoadFactor: 1,
			Local:      true,
		},
		{
			Addr:       "1",
			Port:       1,
			LoadFactor: 2,
			Local:      true,
		},
	}
	anotherEndpoints := []Endpoint{
		{
			Addr: "2",
			Port: 0,
		},
		{
			Addr: "3",
			Port: 1,
		},
	}
	moreEndpointsOverlap := []Endpoint{
		{
			Addr:       "0",
			Port:       0,
			LoadFactor: 1,
			Local:      true,
		},
		{
			Addr: "1",
			Port: 1,
		},
		{
			Addr: "1",
			Port: 2,
		},
	}

	type TC struct {
		name         string
		curr, next   []Endpoint
		eq, add, del int
	}

	tests := []TC{
		{
			name: "none",
			curr: noEndpoints,
			next: noEndpoints,
			eq:   0,
			add:  0,
			del:  0,
		},
		{
			name: "equals",
			curr: someEndpoints,
			next: sameSomeEndpoints,
			eq:   2,
			add:  0,
			del:  0,
		},
		{
			name: "noneToSome",
			curr: noEndpoints,
			next: someEndpoints,
			eq:   0,
			add:  2,
			del:  0,
		},
		{
			name: "SomeToNone",
			curr: someEndpoints,
			next: noEndpoints,
			eq:   0,
			add:  0,
			del:  2,
		},
		{
			name: "SomeToMore",
			curr: someEndpoints,
			next: moreEndpointsOverlap,
			eq:   2,
			add:  1,
			del:  0,
		},
		{
			name: "MoreToSome",
			curr: moreEndpointsOverlap,
			next: someEndpoints,
			eq:   2,
			add:  0,
			del:  1,
		},
		{
			name: "SomeToAnother",
			curr: someEndpoints,
			next: anotherEndpoints,
			eq:   0,
			add:  2,
			del:  2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			eq, add, del := 0, 0, 0
			diffEndpoints(tc.curr, tc.next,
				func(i, j int) { eq++ },
				func(i, j int) { add++ },
				func(i, j int) { del++ },
			)
			if eq != tc.eq || add != tc.add || del != tc.del {
				t.Errorf("Got %d, %d, %d expected: %d, %d, %d", eq, add, del, tc.eq, tc.add, tc.del)
			}
		})
	}
}
