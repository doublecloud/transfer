package ydb

import (
	"testing"
)

var (
	testData = [...]struct {
		name   string
		add    []Endpoint
		del    []Endpoint
		banned map[string]struct{}
		repeat int
		exp    map[string]int
		err    bool
	}{
		{
			add: []Endpoint{
				{Addr: "foo"},
				{Addr: "bar"},
			},
			repeat: 1000,
			exp: map[string]int{
				"foo": 500,
				"bar": 500,
			},
		},
		{
			add: []Endpoint{
				{Addr: "foo", LoadFactor: 0.2},
				{Addr: "bar", LoadFactor: 1},
				{Addr: "baz", LoadFactor: 1},
			},
			repeat: 1000,
			exp: map[string]int{
				"foo": 600,
				"bar": 200,
				"baz": 200,
			},
		},
		{
			add: []Endpoint{
				{Addr: "foo", LoadFactor: 1},
				{Addr: "bar", LoadFactor: 0.1},
				{Addr: "baz", LoadFactor: 0.9},
			},
			repeat: 1000,
			exp: map[string]int{
				"foo": 200,
				"bar": 600,
				"baz": 200,
			},
		},
		{
			add: []Endpoint{
				{Addr: "foo", LoadFactor: 0.25},
				{Addr: "bar", LoadFactor: 1},
				{Addr: "baz", LoadFactor: 1},
			},
			del: []Endpoint{
				{Addr: "foo"},
			},
			repeat: 1000,
			exp: map[string]int{
				"bar": 500,
				"baz": 500,
			},
		},
		{
			add: []Endpoint{
				{Addr: "foo", LoadFactor: 1},
				{Addr: "bar", LoadFactor: 0.25},
				{Addr: "baz", LoadFactor: 0.25},
			},
			del: []Endpoint{
				{Addr: "foo"},
			},
			repeat: 1000,
			exp: map[string]int{
				"bar": 500,
				"baz": 500,
			},
		},
		{
			add: []Endpoint{
				{Addr: "foo", LoadFactor: 1},
				{Addr: "bar", LoadFactor: 0.75},
				{Addr: "baz", LoadFactor: 0.25},
			},
			del: []Endpoint{
				{Addr: "bar"},
			},
			repeat: 1200,
			exp: map[string]int{
				"foo": 400,
				"baz": 800,
			},
		},
		{
			add: []Endpoint{
				{Addr: "foo", LoadFactor: 0},
				{Addr: "bar", LoadFactor: 0},
				{Addr: "baz", LoadFactor: 0},
			},
			del: []Endpoint{
				{Addr: "baz"},
			},
			repeat: 1000,
			exp: map[string]int{
				"foo": 500,
				"bar": 500,
			},
		},
		{
			add: []Endpoint{
				{Addr: "foo", LoadFactor: 0},
				{Addr: "bar", LoadFactor: 0},
				{Addr: "baz", LoadFactor: 0},
			},
			del: []Endpoint{
				{Addr: "foo"},
				{Addr: "bar"},
				{Addr: "baz"},
			},
			repeat: 1,
			err:    true,
		},
		{
			add: []Endpoint{
				{Addr: "foo", LoadFactor: 0},
				{Addr: "bar", LoadFactor: 0},
				{Addr: "baz", LoadFactor: 0},
			},
			banned: map[string]struct{}{
				"foo": {},
				"bar": {},
			},
			repeat: 100,
			err:    true,
			exp: map[string]int{
				"baz": 100,
			},
		},
		{
			add: []Endpoint{
				{Addr: "foo", LoadFactor: 0},
				{Addr: "bar", LoadFactor: 0},
				{Addr: "baz", LoadFactor: 0},
			},
			banned: map[string]struct{}{
				"foo": {},
			},
			repeat: 100,
			err:    true,
			exp: map[string]int{
				"bar": 50,
				"baz": 50,
			},
		},
		{
			add: []Endpoint{
				{Addr: "foo", LoadFactor: 0},
				{Addr: "bar", LoadFactor: 0},
				{Addr: "baz", LoadFactor: 0},
			},
			banned: map[string]struct{}{
				"foo": {},
				"bar": {},
				"baz": {},
			},
			repeat: 150,
			err:    true,
			exp: map[string]int{
				"foo": 50,
				"bar": 50,
				"baz": 50,
			},
		},
		{
			add: []Endpoint{
				{Addr: "foo", LoadFactor: 10},
				{Addr: "bar", LoadFactor: 20},
				{Addr: "baz", LoadFactor: 30},
			},
			banned: map[string]struct{}{
				"foo": {},
				"bar": {},
				"baz": {},
			},
			repeat: 150,
			err:    true,
			exp: map[string]int{
				"foo": 75,
				"bar": 50,
				"baz": 25,
			},
		},
		{
			add: []Endpoint{
				{Addr: "foo", LoadFactor: 10},
				{Addr: "bar", LoadFactor: 20},
				{Addr: "baz", LoadFactor: 30},
			},
			banned: map[string]struct{}{
				"foo": {},
			},
			repeat: 150,
			err:    true,
			exp: map[string]int{
				"foo": 0,
				"bar": 100,
				"baz": 50,
			},
		},
	}
)

func TestRoundRobinBalancer(t *testing.T) {
	for _, test := range testData {
		t.Run(test.name, func(t *testing.T) {
			var (
				mconn = map[*conn]string{} // conn to addr mapping for easy matching.
				maddr = map[string]*conn{} // addr to conn mapping.
				melem = map[string]balancerElement{}
				mdist = map[string]int{}
			)
			r := new(roundRobin)
			for _, e := range test.add {
				c := new(conn)
				c.runtime.setState(ConnOnline)
				mconn[c] = e.Addr
				maddr[e.Addr] = c
				melem[e.Addr] = r.Insert(c, connInfo{
					loadFactor: e.LoadFactor,
				})
			}
			for _, e := range test.del {
				r.Remove(melem[e.Addr])
			}
			for addr := range test.banned {
				if err := r.Pessimize(melem[addr]); err != nil {
					t.Errorf("unexpected pessimization error: %v", err)
				}
			}
			for i := 0; i < test.repeat; i++ {
				conn := r.Next()
				if conn == nil {
					if len(test.add) > len(test.del) {
						t.Fatal("unexpected no-conn")
					}
				} else {
					mdist[mconn[conn]]++
				}
			}
			for addr, exp := range test.exp {
				if act := mdist[addr]; act != exp {
					t.Errorf(
						"unexpected distribution for addr %q: %v; want %v",
						addr, act, exp,
					)
				}
				delete(mdist, addr)
			}
			for addr := range mdist {
				t.Fatalf("unexpected addr in distribution: %q", addr)
			}
		})
	}
}

func TestRandomChoiceBalancer(t *testing.T) {
	for _, test := range testData {
		t.Run(test.name, func(t *testing.T) {
			var (
				mconn = map[*conn]string{} // conn to addr mapping for easy matching.
				maddr = map[string]*conn{} // addr to conn mapping.
				melem = map[string]balancerElement{}
				mdist = map[string]int{}
			)
			r := new(roundRobin)
			for _, e := range test.add {
				c := new(conn)
				c.runtime.setState(ConnOnline)
				mconn[c] = e.Addr
				maddr[e.Addr] = c
				melem[e.Addr] = r.Insert(c, connInfo{
					loadFactor: e.LoadFactor,
				})
			}
			for _, e := range test.del {
				r.Remove(melem[e.Addr])
			}
			for addr := range test.banned {
				if err := r.Pessimize(melem[addr]); err != nil {
					t.Errorf("unexpected pessimization error: %v", err)
				}
			}
			for i := 0; i < test.repeat; i++ {
				conn := r.Next()
				if conn == nil {
					if len(test.add) > len(test.del) {
						t.Fatal("unexpected no-conn")
					}
				} else {
					mdist[mconn[conn]]++
				}
			}
			for addr, exp := range test.exp {
				if act := mdist[addr]; act < int(float64(exp)*0.9) || act > int(float64(exp)*1.1) {
					t.Errorf(
						"unexpected distribution for addr %q: %v; want %v",
						addr, act, exp,
					)
				}
				delete(mdist, addr)
			}
			for addr := range mdist {
				t.Fatalf("unexpected addr in distribution: %q", addr)
			}
		})
	}
}
