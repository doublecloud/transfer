package stats

import "github.com/doublecloud/transfer/library/go/core/metrics"

type ReplicationStats struct {
	StartUnix metrics.Gauge
	Running   metrics.Gauge
}

func NewReplicationStats(registry metrics.Registry) *ReplicationStats {
	return &ReplicationStats{
		StartUnix: registry.Gauge("replication.start.unix"),
		Running:   registry.Gauge("replication.running"),
	}
}
