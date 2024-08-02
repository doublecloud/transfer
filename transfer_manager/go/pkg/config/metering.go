package config

// go-sumtype:decl Metering
type Metering interface {
	TypeTagged
	IsMetering()
}

type MeteringEnabled struct {
	Writers      []WriterEntry `mapstructure:"writers"`
	FatalOnError bool          `mapstructure:"fatal_on_error"`
}

type MeteringDisabled struct{}

func (*MeteringDisabled) IsMetering()   {}
func (*MeteringDisabled) IsTypeTagged() {}

func (*MeteringEnabled) IsMetering()   {}
func (*MeteringEnabled) IsTypeTagged() {}

type WriterEntry struct {
	Writer       WriterConfig      `mapstructure:"writer"`
	MetricTopics map[string]string `mapstructure:"metrics"`
}

type WriterConfig interface {
	TypeTagged
	IsMeteringWriter()
	Type() string
}

func init() {
	RegisterTypeTagged((*Metering)(nil), (*MeteringDisabled)(nil), "disabled", nil)
	RegisterTypeTagged((*Metering)(nil), (*MeteringEnabled)(nil), "enabled", nil)
}
