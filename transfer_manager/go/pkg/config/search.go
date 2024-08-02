package config

type Search interface {
	TypeTagged
	IsSearch()
}

type SearchDisabled struct{}

type SearchEnabled struct {
	Writer WriterConfig `mapstructure:"writer"`
}

func (*SearchDisabled) IsTypeTagged() {}
func (*SearchDisabled) IsSearch()     {}
func (*SearchDisabled) Type() string  { return "disabled" }

func (*SearchEnabled) IsTypeTagged() {}
func (*SearchEnabled) IsSearch()     {}
func (*SearchEnabled) Type() string  { return "enabled" }

func init() {
	RegisterTypeTagged((*Search)(nil), (*SearchEnabled)(nil), (*SearchEnabled)(nil).Type(), nil)
	RegisterTypeTagged((*Search)(nil), (*SearchDisabled)(nil), (*SearchDisabled)(nil).Type(), nil)
}
