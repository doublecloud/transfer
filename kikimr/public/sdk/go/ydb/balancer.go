package ydb

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"
)

// BalancingMethod encodes balancing method for driver configuration.
type BalancingMethod uint

const (
	BalancingUnknown BalancingMethod = iota
	BalancingRoundRobin
	BalancingP2C
	BalancingRandomChoice
)

var (
	// ErrNilBalancerElement returned when requested on a nil balancer element.
	ErrNilBalancerElement = errors.New("nil balancer element")
	// ErrUnknownBalancerElement returned when requested on a unknown balancer element.
	ErrUnknownBalancerElement = errors.New("unknown balancer element")
	// ErrUnknownTypeOfBalancerElement returned when requested on a unknown type of balancer element.
	ErrUnknownTypeOfBalancerElement = errors.New("unknown type of balancer element")
)

// balancerElement is an empty interface that holds some balancer specific
// data.
type balancerElement interface {
}

// balancer is an interface that implements particular load-balancing
// algorithm.
//
// balancer methods called synchronized. That is, implementations must not
// provide additional goroutine safety.
type balancer interface {
	// Next returns next connection for request.
	// Next MUST not return nil if it has at least one connection.
	Next() *conn

	// Insert inserts new connection.
	Insert(*conn, connInfo) balancerElement

	// Update updates previously inserted connection.
	Update(balancerElement, connInfo)

	// Remove removes previously inserted connection.
	Remove(balancerElement)

	// Pessimize pessimizes some balancer element.
	Pessimize(balancerElement) error

	// Contains returns true if balancer contains requested element.
	Contains(balancerElement) bool
}

var balancers = map[BalancingMethod]func(interface{}) balancer{
	BalancingRoundRobin: func(_ interface{}) balancer {
		return new(roundRobin)
	},
	BalancingP2C: func(c interface{}) balancer {
		if c == nil {
			return new(p2c)
		}
		config := c.(*P2CConfig)
		return &p2c{
			Criterion: connRuntimeCriterion{
				PreferLocal:     config.PreferLocal,
				OpTimeThreshold: config.OpTimeThreshold,
			},
		}
	},
	BalancingRandomChoice: func(_ interface{}) balancer {
		return &randomChoice{
			r: rand.New(rand.NewSource(time.Now().UnixNano())),
		}
	},
}

type multiHandle struct {
	elements []balancerElement
}

type multiBalancer struct {
	balancer []balancer
	filter   []func(*conn, connInfo) bool
}

func withBalancer(b balancer, filter func(*conn, connInfo) bool) balancerOption {
	return func(m *multiBalancer) {
		m.balancer = append(m.balancer, b)
		m.filter = append(m.filter, filter)
	}
}

type balancerOption func(*multiBalancer)

func newMultiBalancer(opts ...balancerOption) *multiBalancer {
	m := new(multiBalancer)
	for _, opt := range opts {
		opt(m)
	}
	return m
}

func (m *multiBalancer) Contains(x balancerElement) bool {
	for _, b := range m.balancer {
		if b.Contains(x) {
			return true
		}
	}
	return false
}

func (m *multiBalancer) Next() *conn {
	for _, b := range m.balancer {
		if c := b.Next(); c != nil {
			return c
		}
	}
	return nil
}

func (m *multiBalancer) Insert(conn *conn, info connInfo) balancerElement {
	n := len(m.filter)
	h := multiHandle{
		elements: make([]balancerElement, n),
	}
	for i, f := range m.filter {
		if f(conn, info) {
			x := m.balancer[i].Insert(conn, info)
			h.elements[i] = x
		}
	}
	return h
}

func (m *multiBalancer) Update(x balancerElement, info connInfo) {
	for i, x := range x.(multiHandle).elements {
		if x != nil {
			m.balancer[i].Update(x, info)
		}
	}
}

func (m *multiBalancer) Remove(x balancerElement) {
	for i, x := range x.(multiHandle).elements {
		if x != nil {
			m.balancer[i].Remove(x)
		}
	}
}

func (m *multiBalancer) Pessimize(x balancerElement) error {
	if x == nil {
		return ErrNilBalancerElement
	}
	good := 0
	all := 0
	errs := make([]string, 0)
	for i, x := range x.(multiHandle).elements {
		if x != nil {
			all++
			if e := m.balancer[i].Pessimize(x); e == nil {
				good++
			} else if !errors.Is(e, ErrUnknownBalancerElement) { // collect error only if not an ErrUnknownBalancerElement
				errs = append(errs, e.Error())
			}
		}
	}
	if good > 0 || all == 0 {
		return nil
	}
	return fmt.Errorf("[multiBalancer] unknown balancer element %+v; errors: [ %s ]", x, strings.Join(errs, "; "))
}

type singleConnBalancer struct {
	conn *conn
}

func (s *singleConnBalancer) Next() *conn {
	return s.conn
}
func (s *singleConnBalancer) Insert(conn *conn, _ connInfo) balancerElement {
	if s.conn != nil {
		panic("ydb: single conn balancer: double Insert()")
	}
	s.conn = conn
	return conn
}
func (s *singleConnBalancer) Remove(x balancerElement) {
	if s.conn != x.(*conn) {
		panic("ydb: single conn balancer: Remove() unknown conn")
	}
	s.conn = nil
}
func (s *singleConnBalancer) Update(balancerElement, connInfo)   {}
func (s *singleConnBalancer) Pessimize(el balancerElement) error { return nil }
func (s *singleConnBalancer) Contains(x balancerElement) bool {
	if x == nil {
		return false
	}
	return s.conn != x.(*conn)
}
