package ydb

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
)

const (
	MaxGetConnTimeout    = 10 * time.Second
	ConnResetOfflineRate = uint64(10)
)

var (
	// ErrClusterClosed returned when requested on a closed cluster.
	ErrClusterClosed = errors.New("cluster closed")

	// ErrClusterEmpty returned when no connections left in cluster.
	ErrClusterEmpty = errors.New("cluster empty")

	// ErrUnknownEndpoint returned when no connections left in cluster.
	ErrUnknownEndpoint = errors.New("unknown endpoint")
)

// connInfo contains connection "static" stats – e.g. such that obtained from
// discovery routine.
type connInfo struct {
	loadFactor float32
	local      bool
}

// connEntry represents inserted into the cluster connection.
type connEntry struct {
	conn   *conn
	handle balancerElement

	info connInfo
}

func (c *connEntry) insertInto(b balancer) {
	if c.handle != nil {
		panic("ydb: handle already exists")
	}
	if c.conn == nil {
		panic("ydb: can't insert nil conn into balancer")
	}
	c.handle = b.Insert(c.conn, c.info)
	if c.handle == nil {
		panic("ydb: balancer has returned nil handle")
	}
}

func (c *connEntry) removeFrom(b balancer) {
	if c.handle == nil {
		panic("ydb: no handle to remove from balancer")
	}
	b.Remove(c.handle)
	c.handle = nil
}

type cluster struct {
	dial     func(context.Context, string) (*grpc.ClientConn, error)
	balancer balancer
	explorer *repeater
	trace    DriverTrace

	mu    sync.RWMutex
	index map[connAddr]connEntry

	closed bool
}

func (c *cluster) Close() (err error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	if c.explorer != nil {
		c.explorer.Stop()
	}
	c.closed = true

	index := c.index
	c.index = nil

	c.mu.Unlock()

	for _, entry := range index {
		_ = entry.conn.close()
	}

	return
}

// Get returns next available connection.
// It returns error on given context cancellation or when cluster become closed.
func (c *cluster) Get(ctx context.Context) (cc *conn, err error) {
	if err = ctx.Err(); err != nil {
		return nil, err
	}

	onDone := driverTraceOnGetConn(ctx, c.trace, ctx)
	defer func() {
		onDone(ctx, cc.Address(), err)
	}()

	c.mu.RLock()
	closed := c.closed
	c.mu.RUnlock()
	if closed {
		return nil, ErrClusterClosed
	}

	if endpointInfo := ContextEndpointInfo(ctx); endpointInfo != nil {
		c.mu.RLock()
		entry, ok := c.index[connAddrFromString(endpointInfo.Address())]
		c.mu.RUnlock()
		if ok {
			return entry.conn, nil
		}
	}

	c.mu.Lock()
	cc = c.balancer.Next()
	c.mu.Unlock()
	if cc == nil {
		return nil, ErrClusterEmpty
	}
	return cc, nil
}

// Insert inserts new connection into the cluster.
func (c *cluster) Insert(ctx context.Context, e Endpoint, wg ...WG) {
	if len(wg) > 0 {
		defer wg[0].Done()
	}

	addr := connAddr{e.Addr, e.Port}
	info := connInfo{
		loadFactor: e.LoadFactor,
		local:      e.Local,
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return
	}
	_, has := c.index[addr]
	if has {
		panic("ydb: can't insert already existing endpoint")
	}
	entry := connEntry{
		conn: newConn(addr, c.dial),
		info: info,
	}
	entry.insertInto(c.balancer)
	c.index[addr] = entry
}

// Update updates existing connection's runtime stats such that load factor and
// others.
func (c *cluster) Update(ctx context.Context, ep Endpoint, wg ...WG) {
	if len(wg) > 0 {
		defer wg[0].Done()
	}

	addr := connAddr{ep.Addr, ep.Port}
	info := connInfo{
		loadFactor: ep.LoadFactor,
		local:      ep.Local,
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}

	entry, has := c.index[addr]
	if !has {
		panic("ydb: can't update not-existing endpoint")
	}

	entry.info = info
	if entry.conn != nil {
		entry.conn.runtime.setState(ConnOnline)
	}
	c.index[addr] = entry
	if entry.handle != nil {
		// entry.handle may be nil when connection is being tracked.
		c.balancer.Update(entry.handle, info)
	}
}

// Remove removes and closes previously inserted connection.
func (c *cluster) Remove(_ context.Context, e Endpoint, wg ...WG) {
	if len(wg) > 0 {
		defer wg[0].Done()
	}

	addr := connAddr{e.Addr, e.Port}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}

	entry, has := c.index[addr]
	if !has {
		c.mu.Unlock()
		panic("ydb: can't remove not-existing endpoint")
	}
	entry.removeFrom(c.balancer)
	delete(c.index, addr)
	c.mu.Unlock()

	_ = entry.conn.close()
}

func (c *cluster) Pessimize(addr connAddr) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClusterClosed
	}

	entry, has := c.index[addr]
	if !has {
		return ErrUnknownEndpoint
	}
	if entry.handle == nil {
		return ErrNilBalancerElement
	}
	if !c.balancer.Contains(entry.handle) {
		return ErrUnknownBalancerElement
	}
	err = c.balancer.Pessimize(entry.handle)
	if err == nil && c.explorer != nil {
		// count ratio (banned/all)
		online := 0
		for _, e := range c.index {
			if e.conn != nil && e.conn.runtime.getState() == ConnOnline {
				online++
			}
		}
		// more then half connections banned - re-discover now
		if online*2 < len(c.index) {
			c.explorer.Force()
		}
	}
	return err
}

func (c *cluster) Stats(it func(Endpoint, ConnStats)) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return
	}
	call := func(conn *conn, info connInfo) {
		e := Endpoint{
			Addr:       conn.addr.addr,
			Port:       conn.addr.port,
			LoadFactor: info.loadFactor,
			Local:      info.local,
		}
		s := conn.runtime.stats()
		it(e, s)
	}
	for _, entry := range c.index {
		if entry.conn != nil {
			call(entry.conn, entry.info)
		}
	}
}

func compareEndpoints(a, b Endpoint) int {
	if c := strings.Compare(a.Addr, b.Addr); c != 0 {
		return c
	}
	if c := a.Port - b.Port; c != 0 {
		return c
	}
	return 0
}

func sortEndpoints(es []Endpoint) {
	sort.Slice(es, func(i, j int) bool {
		return compareEndpoints(es[i], es[j]) < 0
	})
}

func diffEndpoints(curr, next []Endpoint, eq, add, del func(i, j int)) {
	diffslice(
		len(curr),
		len(next),
		func(i, j int) int {
			return compareEndpoints(curr[i], next[j])
		},
		func(i, j int) {
			eq(i, j)
		},
		func(i, j int) {
			add(i, j)
		},
		func(i, j int) {
			del(i, j)
		},
	)
}

func diffslice(a, b int, cmp func(i, j int) int, eq, add, del func(i, j int)) {
	var i, j int
	for i < a && j < b {
		c := cmp(i, j)
		switch {
		case c < 0:
			del(i, j)
			i++
		case c > 0:
			add(i, j)
			j++
		default:
			eq(i, j)
			i++
			j++
		}
	}
	for ; i < a; i++ {
		del(i, j)
	}
	for ; j < b; j++ {
		add(i, j)
	}
}

type connListElement struct {
	index int
	conn  *conn
	info  connInfo
}

type connList []*connListElement

func (cs *connList) Insert(conn *conn, info connInfo) *connListElement {
	e := &connListElement{
		index: len(*cs),
		conn:  conn,
		info:  info,
	}
	*cs = append(*cs, e)
	return e
}

func (cs *connList) Remove(x *connListElement) {
	list := *cs
	var (
		n    = len(list)
		last = list[n-1]
	)
	last.index = x.index
	list[x.index], list[n-1] = list[n-1], nil
	list = list[:n-1]
	*cs = list
}

func (cs *connList) Contains(x *connListElement) bool {
	l := *cs
	var (
		n = len(l)
	)
	if x.index >= n {
		return false
	}
	return l[x.index] == x
}
