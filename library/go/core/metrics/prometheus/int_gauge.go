package prometheus

import (
	"github.com/doublecloud/transfer/library/go/core/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var _ metrics.IntGauge = (*IntGauge)(nil)

// IntGauge tracks single int64 value.
type IntGauge struct {
	metrics.Gauge
}

func (i IntGauge) Set(value int64) {
	i.Gauge.Set(float64(value))
}

func (i IntGauge) Add(value int64) {
	i.Gauge.Add(float64(value))
}

var _ metrics.FuncIntGauge = (*FuncIntGauge)(nil)

type FuncIntGauge struct {
	ff       prometheus.GaugeFunc
	function func() int64
}

func (g FuncIntGauge) Function() func() int64 {
	return g.function
}
