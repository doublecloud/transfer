package config

type Metrika interface {
	TypeTagged
	isMetrika()
}

type MetrikaDisabled struct{}

func (m *MetrikaDisabled) IsTypeTagged() {}

func (m *MetrikaDisabled) isMetrika() {}

type MetrikaAPIConfig struct {
	Endpoint     string `mapstructure:"endpoint"`
	TVMSelfID    int    `mapstructure:"tvm_self_id"`
	TVMMetrikaID int    `mapstructure:"tvm_metrika_id"`
	TVMSecret    Secret `mapstructure:"tvm_secret"`
}

type MetrikaTopicsConfig struct {
	Endpoint           string     `mapstructure:"endpoint"`
	Database           string     `mapstructure:"database"`
	Port               int        `mapstructure:"port"`
	Creds              CloudCreds `mapstructure:"creds"`
	IAMTokenServiceAPI string     `mapstructure:"iam_token_service_api"`
}

type MetrikaEnabled struct {
	API    MetrikaAPIConfig    `mapstructure:"api"`
	Topics MetrikaTopicsConfig `mapstructure:"topics"`
}

func (m *MetrikaEnabled) IsTypeTagged() {}

func (m *MetrikaEnabled) isMetrika() {}

func init() {
	RegisterTypeTagged((*Metrika)(nil), (*MetrikaDisabled)(nil), "disabled", nil)
	RegisterTypeTagged((*Metrika)(nil), (*MetrikaEnabled)(nil), "enabled", nil)
}
