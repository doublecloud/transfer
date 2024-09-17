package stats

import (
	"fmt"

	"github.com/doublecloud/transfer/library/go/core/metrics"
)

type AuthStats struct {
	stats map[authMetricKey]metrics.Counter
}

type authMetricKey struct {
	authSuccess bool
}

func (s *AuthStats) Add(authSuccess bool) {
	key := authMetricKey{
		authSuccess: authSuccess,
	}
	s.stats[key].Inc()
}

func NewAuthStats(registry metrics.Registry) *AuthStats {
	stats := make(map[authMetricKey]metrics.Counter)
	for _, authSuccess := range []bool{false, true} {
		subRegistry := registry.WithTags(map[string]string{"success": fmt.Sprintf("%t", authSuccess)})
		stats[authMetricKey{authSuccess: authSuccess}] = subRegistry.Counter("auth")
	}

	return &AuthStats{stats: stats}
}
