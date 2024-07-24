package ydb

import (
	"container/heap"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
)

// roundRobin is an implementation of weighted round-robin balancing algorithm.
//
// It relies on connection's load factor (usually obtained by discovery
// routine – that is, not a runtime metric) and interprets it as inversion of
// weight.
type roundRobin struct {
	min   float32
	max   float32
	belt  []int
	next  int32
	conns connList
}

type randomChoice struct {
	roundRobin
	r *rand.Rand // without seed by default
	m sync.Mutex
}

func (r *roundRobin) Next() *conn {
	if n := len(r.conns); n == 0 {
		return nil
	}
	d := int(atomic.AddInt32(&r.next, 1)) % len(r.belt)
	i := r.belt[d]
	return r.conns[i].conn
}

func (r *randomChoice) Next() *conn {
	if n := len(r.conns); n == 0 {
		return nil
	}
	r.m.Lock()
	i := r.belt[r.r.Intn(len(r.belt))]
	r.m.Unlock()
	return r.conns[i].conn
}

func (r *roundRobin) Insert(conn *conn, info connInfo) balancerElement {
	e := r.conns.Insert(conn, info)
	r.updateMinMax(info)
	r.belt = r.distribute()
	return e
}

func (r *roundRobin) Update(el balancerElement, info connInfo) {
	e := el.(*connListElement)
	e.info = info
	r.updateMinMax(info)
	r.belt = r.distribute()
}

func (r *roundRobin) Remove(x balancerElement) {
	el := x.(*connListElement)
	r.conns.Remove(el)
	r.inspectMinMax(el.info)
	r.belt = r.distribute()
}

func (r *roundRobin) Pessimize(x balancerElement) error {
	if x == nil {
		return ErrNilBalancerElement
	}
	el, ok := x.(*connListElement)
	if !ok {
		return ErrUnknownTypeOfBalancerElement
	}
	if !r.conns.Contains(el) {
		return ErrUnknownBalancerElement
	}
	el.conn.runtime.setState(ConnBanned)
	r.belt = r.distribute()
	return nil
}

func (r *roundRobin) Contains(x balancerElement) bool {
	if x == nil {
		return false
	}
	el, ok := x.(*connListElement)
	if !ok {
		return false
	}
	return r.conns.Contains(el)
}

func (r *roundRobin) updateMinMax(info connInfo) {
	if len(r.conns) == 1 {
		r.min = info.loadFactor
		r.max = info.loadFactor
		return
	}
	if info.loadFactor < r.min {
		r.min = info.loadFactor
	}
	if info.loadFactor > r.max {
		r.max = info.loadFactor
	}
}

func (r *roundRobin) inspectMinMax(info connInfo) {
	if r.min != info.loadFactor && r.max != info.loadFactor {
		return
	}
	var def bool
	for _, x := range r.conns {
		load := x.info.loadFactor
		if !def {
			r.min = load
			r.max = load
			def = true
		}
		if load < r.min {
			r.min = load
		}
		if load > r.max {
			r.max = load
		}
	}
}

func (r *roundRobin) distribute() []int {
	return r.spread(distribution(
		r.min, int32(len(r.conns)),
		r.max, 1,
	))
}

func (r *roundRobin) spread(f func(float32) int32) []int {
	var (
		dist  = make([]int32, 0, len(r.conns))
		index = make([]int, 0, len(r.conns))
	)
	fill := func(state ConnState) (filled bool) {
		for _, x := range r.conns {
			if x.conn.runtime.getState() == state {
				d := f(x.info.loadFactor)
				dist = append(dist, d)
				index = append(index, x.index)
				filled = true
			}
		}
		return filled
	}
	for _, s := range [...]ConnState{
		ConnOnline,
		ConnBanned,
		ConnStateUnknown,
		ConnOffline,
	} {
		if fill(s) {
			return genBelt(index, dist)
		}
	}
	return nil
}

func genBelt(index []int, weight []int32) (r []int) {
	h := make(distItemsHeap, len(weight))
	for i, w := range weight {
		h[i] = newDistItem(index[i], w)
	}
	heap.Init(&h)
	for len(h) > 0 {
		x := heap.Pop(&h).(*distItem)
		r = append(r, x.index())
		if x.tick() {
			heap.Push(&h, x)
		}
	}
	return
}

func distribution(x1 float32, y1 int32, x2 float32, y2 int32) (f func(float32) int32) {
	if x1 == x2 {
		f = func(float32) int32 { return 1 }
	} else {
		a := float32(y2-y1) / (x2 - x1)
		b := float32(y1) - a*x1
		f = func(x float32) int32 {
			return int32(math.Round(float64(a*x + b)))
		}
	}
	return f
}

type distItem struct {
	i     int
	step  float64
	value float64
}

// newDistItem creates new distribution item.
// w must be greater than zero.
func newDistItem(i int, w int32) *distItem {
	step := 1 / float64(w)
	return &distItem{
		i:     i,
		step:  step,
		value: step,
	}
}

func (x *distItem) tick() bool {
	x.value += x.step
	return x.value <= 1
}

func (x *distItem) index() int {
	return x.i
}

type distItemsHeap []*distItem

func (h distItemsHeap) Len() int           { return len(h) }
func (h distItemsHeap) Less(i, j int) bool { return h[i].value < h[j].value }
func (h distItemsHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *distItemsHeap) Push(x interface{}) {
	*h = append(*h, x.(*distItem))
}

func (h *distItemsHeap) Pop() interface{} {
	p := *h
	n := len(p)
	x := p[n-1]
	*h = p[:n-1]
	return x
}
