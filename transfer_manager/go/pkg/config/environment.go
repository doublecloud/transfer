package config

// go-sumtype:decl Environment
type Environment interface {
	TypeTagged
	isEnvironment()
}

type Internal struct{}
type YC struct{}
type AWS struct{}
type Nebius struct{}

func (*Internal) isEnvironment() {}
func (*Internal) IsTypeTagged()  {}

func (*YC) isEnvironment() {}
func (*YC) IsTypeTagged()  {}

func (*AWS) isEnvironment() {}
func (*AWS) IsTypeTagged()  {}

func (*Nebius) isEnvironment() {}
func (*Nebius) IsTypeTagged()  {}

func init() {
	RegisterTypeTagged((*Environment)(nil), (*Internal)(nil), "internal", nil)
	RegisterTypeTagged((*Environment)(nil), (*YC)(nil), "yc", nil)
	RegisterTypeTagged((*Environment)(nil), (*AWS)(nil), "aws", nil)
	RegisterTypeTagged((*Environment)(nil), (*Nebius)(nil), "nebius", nil)
}
