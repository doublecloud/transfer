package config

// go-sumtype:decl HealthProvider
type HealthProvider interface {
	TypeTagged
	HealthProviderType() string
}

type HealthProviderFake struct{}

func (*HealthProviderFake) IsTypeTagged()              {}
func (*HealthProviderFake) HealthProviderType() string { return healthProviderFakeType }

const healthProviderFakeType string = "fake"

func init() {
	RegisterTypeTagged((*HealthProvider)(nil), (*HealthProviderFake)(nil), healthProviderFakeType, nil)
}

type HealthProviderSolomon struct {
	AlertID   string `mapstructure:"alert_id"`
	AlertName string `mapstructure:"name"`
}

func (*HealthProviderSolomon) IsTypeTagged()              {}
func (*HealthProviderSolomon) HealthProviderType() string { return healthProviderSolomonType }

const healthProviderSolomonType string = "solomon"

func init() {
	RegisterTypeTagged((*HealthProvider)(nil), (*HealthProviderSolomon)(nil), healthProviderSolomonType, nil)
}

type HealthProviderPrometheus struct{}

func (*HealthProviderPrometheus) IsTypeTagged()              {}
func (*HealthProviderPrometheus) HealthProviderType() string { return healthProviderPrometheusType }

const healthProviderPrometheusType string = "prometheus"

func init() {
	RegisterTypeTagged((*HealthProvider)(nil), (*HealthProviderPrometheus)(nil), healthProviderPrometheusType, nil)
}

type HealthProviderStatusMessage struct{}

func (*HealthProviderStatusMessage) IsTypeTagged() {}
func (*HealthProviderStatusMessage) HealthProviderType() string {
	return healthProviderStatusMessageType
}

const healthProviderStatusMessageType string = "status_message"

func init() {
	RegisterTypeTagged((*HealthProvider)(nil), (*HealthProviderStatusMessage)(nil), healthProviderStatusMessageType, nil)
}

type HealthProviderOperationStatus struct{}

func (*HealthProviderOperationStatus) IsTypeTagged() {}
func (*HealthProviderOperationStatus) HealthProviderType() string {
	return healthProviderOperationStatusType
}

const healthProviderOperationStatusType string = "operation_status"

func init() {
	RegisterTypeTagged((*HealthProvider)(nil), (*HealthProviderOperationStatus)(nil), healthProviderOperationStatusType, nil)
}
