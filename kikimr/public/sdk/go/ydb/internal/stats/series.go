package stats

import (
	"bytes"
	"fmt"
	"time"
)

type bucket struct {
	sum float64
	cnt int64
}

func (b *bucket) add(x bucket) {
	b.sum += x.sum
	b.cnt += x.cnt
}

// Series contains logic of accumulating time series data.
type Series struct {
	buckets []bucket
	zero    []bucket

	duration time.Duration
	touched  time.Time
}

// NewSeries creates time series aggregation with d window size and granularity
// rate n.
//
// The higher the granulatiry, the more accurate and smooth results will be
// provided by series.
func NewSeries(d time.Duration, n int) *Series {
	if d <= 0 {
		panic("ydb: internal: stats: zero or negative duration for series")
	}
	if n <= 0 {
		panic("ydb: internal: stats: zero or negative granularity for series")
	}
	return &Series{
		buckets:  make([]bucket, n),
		zero:     make([]bucket, n),
		duration: d,
	}
}

// Add adds given value x at the time moment of now.
func (s *Series) Add(now time.Time, x float64) {
	s.buckets[s.rotate(now)].add(bucket{
		sum: x,
		cnt: 1,
	})
}

// Get returns accumulated data available at the moment of now.
func (s *Series) Get(now time.Time) (sum float64, cnt int64) {
	var (
		total = bucket{}
	)
	for i := s.rotate(now); i < len(s.buckets); i++ {
		total.add(s.buckets[i])
	}
	return total.sum, total.cnt
}

// SumPer returns rate of accumulated data available at the moment of now.
func (s *Series) SumPer(now time.Time, period time.Duration) float64 {
	sum, _ := s.Get(now)
	return sum * float64(period) / float64(s.duration)
}

func (s *Series) rotate(now time.Time) (nowBucket int) {
	if s.touched.IsZero() {
		s.touched = now
		return 0
	}

	var (
		d     = now.Sub(s.touched) // Elapsed duration.
		n     = len(s.buckets)
		span  = s.duration / time.Duration(n)
		shift = int(d / span)
	)

	if shift == 0 {
		return 0
	}

	// Slide window strictly on the span size grid.
	// only to the future
	if shift > 0 {
		s.touched = s.touched.Add(time.Duration(shift) * span)
	}

	if shift >= n || shift < -n+1 {
		copy(s.buckets, s.zero)
		return 0
	}

	// bucket in the past
	if shift < 0 {
		return -shift
	}

	// shift buckets front to tail
	copy(s.buckets[shift:], s.buckets)

	// zerois skips
	copy(s.buckets, s.zero[n-shift:])

	return shift - 1
}

func (s *Series) String() string {
	var (
		buf   bytes.Buffer
		total = bucket{}
	)
	for i, b := range s.buckets {
		if i > 0 {
			fmt.Fprint(&buf, ", ")
		}
		fmt.Fprintf(&buf, "[%0.f %d]", b.sum, b.cnt)
		total.add(b)
	}
	fmt.Fprintf(&buf, " = %0.f %d\n", total.sum, total.cnt)
	return buf.String()
}
