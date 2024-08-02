package config

// go-sumtype:decl MetricsProvider
type MetricsProvider interface {
	TypeTagged
	MetricsProviderType() string
}

type MetricsProviderFake struct{}

func (*MetricsProviderFake) IsTypeTagged()               {}
func (*MetricsProviderFake) MetricsProviderType() string { return metricsProviderFakeType }

const metricsProviderFakeType string = "fake"

func init() {
	RegisterTypeTagged((*MetricsProvider)(nil), (*MetricsProviderFake)(nil), metricsProviderFakeType, nil)
}

type MetricsProviderSolomon struct{}

func (*MetricsProviderSolomon) IsTypeTagged()               {}
func (*MetricsProviderSolomon) MetricsProviderType() string { return metricsProviderSolomonType }

const metricsProviderSolomonType string = "solomon"

func init() {
	RegisterTypeTagged((*MetricsProvider)(nil), (*MetricsProviderSolomon)(nil), metricsProviderSolomonType, nil)
}

type MetricsProviderPrometheus struct{}

func (*MetricsProviderPrometheus) IsTypeTagged()               {}
func (*MetricsProviderPrometheus) MetricsProviderType() string { return metricsProviderPrometheusType }

const metricsProviderPrometheusType string = "prometheus"

func init() {
	RegisterTypeTagged((*MetricsProvider)(nil), (*MetricsProviderPrometheus)(nil), metricsProviderPrometheusType, nil)
}
