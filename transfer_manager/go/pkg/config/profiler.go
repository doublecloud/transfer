package config

// go-sumtype:decl Profiler
type Profiler interface {
	TypeTagged
	isProfiler()
}

type ProfilerDisabled struct{}

func (*ProfilerDisabled) isProfiler()   {}
func (*ProfilerDisabled) IsTypeTagged() {}

func (*LogLogbroker) isProfiler() {}

func init() {
	RegisterTypeTagged((*Profiler)(nil), (*ProfilerDisabled)(nil), "disabled", nil)
	RegisterTypeTagged((*Profiler)(nil), (*LogLogbroker)(nil), "logbroker", nil)
}
