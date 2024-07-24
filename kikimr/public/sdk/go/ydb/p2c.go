package ydb

import (
	"math/rand"
	"sync"
	"time"
)

type P2CConfig struct {
	// PreferLocal reports whether p2c balancer should prefer local endpoint
	// when all other runtime indicators are the same (such as error rate or
	// average response time).
	PreferLocal bool

	// OpTimeThreshold specifies such difference between endpoint average
	// operation time when it becomes significant to be used during comparison.
	OpTimeThreshold time.Duration
}

type criterion interface {
	Best(a, b *connListElement) *connListElement
}

type connRuntimeCriterion struct {
	PreferLocal     bool
	OpTimeThreshold time.Duration
}

func (c connRuntimeCriterion) chooseByState(c1, c2 *connListElement, s1, s2 ConnState) *connListElement {
	if s1 == s2 {
		return nil
	}
	switch s1 {
	case ConnStateUnknown:
	case ConnOnline:
	case ConnOffline:
	case ConnBanned:
	}
	if s1 > s2 {
		return c1
	} else if s2 > s1 {
		return c2
	} else {
		return nil
	}
}

func (c connRuntimeCriterion) Best(c1, c2 *connListElement) *connListElement {
	s1 := c1.conn.runtime.stats()
	s2 := c2.conn.runtime.stats()

	if choise := c.chooseByState(c1, c2, s1.State, s2.State); choise != nil {
		return choise
	}

	var (
		f1 float64
		f2 float64
	)
	if s1.OpPerMinute > 0 {
		f1 = s1.ErrPerMinute / s1.OpPerMinute
	}
	if s2.OpPerMinute > 0 {
		f2 = s2.ErrPerMinute / s2.OpPerMinute
	}
	if f1 == f2 {
		t := s1.AvgOpTime - s2.AvgOpTime
		switch {
		case absDuration(t) > c.OpTimeThreshold:
			if t < 0 {
				f1 = 0
				f2 = 1
			} else {
				f1 = 1
				f2 = 0
			}
		case c.PreferLocal && c1.info.local && !c2.info.local:
			f1 = 0
			f2 = 1
		case c.PreferLocal && c2.info.local && !c1.info.local:
			f1 = 1
			f2 = 0
		default:
			f1 = float64(s1.OpPending())
			f2 = float64(s2.OpPending())
		}
	}
	if f1 < f2 {
		return c1
	}
	return c2
}

// p2c implements the "power of two choices" balancing algorithm.
// See https://www.eecs.harvard.edu/~michaelm/postscripts/mythesis.pdf
type p2c struct {
	Source    rand.Source64
	Criterion criterion

	once sync.Once
	rand *rand.Rand

	conns connList
}

func (p *p2c) init() {
	p.once.Do(func() {
		if p.Criterion == nil {
			p.Criterion = &connRuntimeCriterion{
				OpTimeThreshold: time.Second,
			}
		}
		if p.Source == nil {
			p.Source = rand.NewSource(0).(rand.Source64)
		}
		p.rand = rand.New(&lockedSource{src: p.Source})
	})
}

func (p *p2c) Next() *conn {
	p.init()

	n := len(p.conns)
	switch n {
	case 0:
		return nil
	case 1:
		return p.conns[0].conn
	}

	var (
		r1 uint64
		r2 uint64
	)
	const (
		maxRetries = 2
	)
	for i := 0; i <= maxRetries; i++ {
		rnd := p.rand.Uint64()
		r1 = (rnd >> 32) % uint64(n)
		r2 = (rnd & 0xffffffff) % uint64(n)
		if r1 != r2 {
			break
		}
	}

	b := p.Criterion.Best(p.conns[r1], p.conns[r2])

	return b.conn
}

func (p *p2c) Insert(conn *conn, info connInfo) balancerElement {
	el := p.conns.Insert(conn, info)
	return el
}

func (p *p2c) Update(x balancerElement, info connInfo) {
	el := x.(*connListElement)
	el.info = info
}

func (p *p2c) Remove(x balancerElement) {
	p.conns.Remove(x.(*connListElement))
}

func (p *p2c) Pessimize(x balancerElement) error {
	if x == nil {
		return ErrNilBalancerElement
	}
	el, ok := x.(*connListElement)
	if !ok {
		return ErrUnknownTypeOfBalancerElement
	}
	if !p.conns.Contains(el) {
		return ErrUnknownBalancerElement
	}
	el.conn.runtime.setState(ConnBanned)
	return nil
}

func (p *p2c) Contains(x balancerElement) bool {
	if x == nil {
		return false
	}
	el, ok := x.(*connListElement)
	if !ok {
		return false
	}
	return p.conns.Contains(el)
}

type lockedSource struct {
	mu  sync.Mutex
	src rand.Source64
}

func (s *lockedSource) Int63() int64 {
	s.mu.Lock()
	x := s.src.Int63()
	s.mu.Unlock()
	return x
}
func (s *lockedSource) Uint64() uint64 {
	s.mu.Lock()
	x := s.src.Uint64()
	s.mu.Unlock()
	return x
}
func (s *lockedSource) Seed(seed int64) {
	s.mu.Lock()
	s.src.Seed(seed)
	s.mu.Unlock()
}

func absDuration(d time.Duration) time.Duration {
	x := int64(d)
	m := x >> 63
	return time.Duration(x ^ m - m)
}
