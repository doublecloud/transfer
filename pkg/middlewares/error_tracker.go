package middlewares

import (
	"github.com/doublecloud/transfer/library/go/core/metrics"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/errors"
	"github.com/doublecloud/transfer/pkg/errors/categories"
	"github.com/doublecloud/transfer/pkg/stats"
)

// ErrorTracker do nothing except tracking error / success pushes into metrics
func ErrorTracker(mtrcs metrics.Registry) func(abstract.Sinker) abstract.Sinker {
	return func(s abstract.Sinker) abstract.Sinker {
		return newErrorTracker(s, mtrcs)
	}
}

type errorTracker struct {
	sink  abstract.Sinker
	stats *stats.MiddlewareErrorTrackerStats
}

func newErrorTracker(s abstract.Sinker, mtrcs metrics.Registry) *errorTracker {
	return &errorTracker{
		sink:  s,
		stats: stats.NewMiddlewareErrorTrackerStats(mtrcs),
	}
}

func (r *errorTracker) Close() error {
	return r.sink.Close()
}

func (r *errorTracker) Push(input []abstract.ChangeItem) error {
	if err := r.sink.Push(input); err != nil {
		r.stats.Failures.Inc()
		return errors.CategorizedErrorf(categories.Target, "Push failed: %w", err)
	}
	r.stats.Successes.Inc()
	return nil
}
