package stats

var DefaultMetrics = map[string]string{
	"publisher.consumer.fatal":           "Number of fatal errors on the consumer. Once a fatal error occurs, the reader process is eventually closed",
	"publisher.consumer.active":          "Number of active reader processes",
	"publisher.data.changeitems":         "Number of change-items (i.e. events) emitted by the source",
	"sinker.pusher.data.changeitems":     "Number of change-items (i.e. events) written to the target DB",
	"sinker.pusher.time.row_max_lag_sec": "Maximum change-item lag in seconds",
	"sinker.table.rows":                  "Number of rows written per data object (table)",
	"sinker.time.push":                   "Average time to push data to the target",
}
