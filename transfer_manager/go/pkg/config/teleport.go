package config

// Teleport is a configuration of https://goteleport.com/
type Teleport interface {
	TypeTagged
	isTeleport()
}

type TeleportDisabled struct{}

var _ Teleport = (*TeleportDisabled)(nil)

func (*TeleportDisabled) IsTypeTagged() {}
func (*TeleportDisabled) isTeleport()   {}
func (*TeleportDisabled) Type() string  { return "disabled" }

func init() {
	RegisterTypeTagged((*Teleport)(nil), (*TeleportDisabled)(nil), (*TeleportDisabled)(nil).Type(), nil)
}

type TeleportEnabled struct {
	// Telepass is an in-house service to obtain credentials for Teleport agent's authentication
	TelepassURL string            `mapstructure:"telepass_url"`
	Proxy       string            `mapstructure:"proxy"`
	Labels      map[string]string `mapstructure:"labels"`
}

var _ Teleport = (*TeleportEnabled)(nil)

func (*TeleportEnabled) IsTypeTagged() {}
func (*TeleportEnabled) isTeleport()   {}
func (*TeleportEnabled) Type() string  { return "enabled" }

func init() {
	RegisterTypeTagged((*Teleport)(nil), (*TeleportEnabled)(nil), (*TeleportEnabled)(nil).Type(), nil)
}
