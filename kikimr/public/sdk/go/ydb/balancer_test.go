package ydb

import (
	"strconv"
	"testing"
)

func isEvenConn(c *conn, _ connInfo) bool {
	n, err := strconv.Atoi(c.addr.addr)
	if err != nil {
		panic(err)
	}
	return n%2 == 0
}

func isOddConn(c *conn, info connInfo) bool {
	return !isEvenConn(c, info)
}

func TestMultiBalancer(t *testing.T) {
	cs1, b1 := simpleBalancer()
	cs2, b2 := simpleBalancer()
	forEachList := func(it func(*connList)) {
		it(cs1)
		it(cs2)
	}
	forEachConn := func(it func(*conn, connInfo)) {
		forEachList(func(cs *connList) {
			for _, e := range *cs {
				it(e.conn, e.info)
			}
		})
	}
	m := newMultiBalancer(
		withBalancer(b1, isOddConn),
		withBalancer(b2, isEvenConn),
	)
	const n = 100
	var (
		es = make([]balancerElement, n)
		el = make(map[*conn]balancerElement, n)
	)
	for i := 0; i < n; i++ {
		c := &conn{
			addr: connAddr{
				addr: strconv.Itoa(i),
			},
		}
		e := m.Insert(c, connInfo{})
		es[i] = e
		el[c] = e
	}
	forEachList(func(cs *connList) {
		if act, exp := len(*cs), n/2; act != exp {
			t.Errorf(
				"unexepcted number of conns: %d; want %d",
				act, exp,
			)
		}
	})
	for i := 0; i < n; i++ {
		m.Update(es[i], connInfo{
			loadFactor: 1,
		})
	}
	forEachConn(func(conn *conn, info connInfo) {
		if act, exp := info.loadFactor, float32(1); act != exp {
			t.Errorf(
				"unexpected load factor: %f; want %f",
				act, exp,
			)
		}
	})

	// Multibalancer must check first balancer first.
	// Thus, we expect here that until first balancer is not empty
	// multibalancer will return connections only from it.
	for i := 0; i < n; i++ {
		c := m.Next()
		if !isOddConn(c, connInfo{}) {
			t.Fatalf("Next() returned unexpected conn")
		}
	}
	// Now remove all connections from first balancer.
	for i := 0; i < n/2; i++ {
		c := m.Next()
		if isOddConn(c, connInfo{}) {
			m.Remove(el[c])
		}
	}
	// And check that multibalancer returns connections from the second
	// balancer.
	for i := 0; i < n; i++ {
		c := m.Next()
		if !isEvenConn(c, connInfo{}) {
			t.Fatalf("Next() returned unexpected conn")
		}
	}
	// Now remove all connections from second balancer.
	for i := 0; i < n/2; i++ {
		c := m.Next()
		if isEvenConn(c, connInfo{}) {
			m.Remove(el[c])
		}
	}
	if c := m.Next(); c != nil {
		t.Fatalf("Next() returned unexpected non-nil connection")
	}
}
