package config

// go-sumtype:decl Events
type Events interface {
	TypeTagged
	isEvents()
}

type EventsDisabled struct{}

func (m *EventsDisabled) IsTypeTagged() {}
func (m *EventsDisabled) isEvents()     {}

type EventsAuditTrails struct{}

func (m *EventsAuditTrails) IsTypeTagged() {}
func (m *EventsAuditTrails) isEvents()     {}

func init() {
	RegisterTypeTagged((*Events)(nil), (*EventsDisabled)(nil), "disabled", nil)
	RegisterTypeTagged((*Events)(nil), (*EventsAuditTrails)(nil), "audit_trails", nil)
}
