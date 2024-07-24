package testutil

import (
	"time"
)

type StubTicker struct {
	d time.Duration
	f func(time.Time)
}

func (s *StubTicker) Reset(d time.Duration, f func(time.Time)) {
	s.d = d
	s.f = f
}

func (s *StubTicker) Emit(t time.Time) {
	s.f(t)
}

func (s StubTicker) Destroy() {}
