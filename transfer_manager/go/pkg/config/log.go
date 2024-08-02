package config

// go-sumtype:decl Log
type Log interface {
	TypeTagged
	isLog()
}

type LogStdout struct{}

type LogLogbroker struct {
	Endpoint    string               `mapstructure:"endpoint"`
	Creds       LogbrokerCredentials `mapstructure:"creds"`
	Database    string               `mapstructure:"database"`
	Topic       string               `mapstructure:"topic"`
	TLSMode     TLSMode              `mapstructure:"tls_mode"`
	SourceID    string               `mapstructure:"source_id,local_only"`
	UseTopicAPI bool                 `mapstructure:"use_topic_api"`
}

type LogKafka struct {
	Brokers  string  `mapstructure:"brokers"`
	User     string  `mapstructure:"user"`
	Password Secret  `mapstructure:"password"`
	Topic    string  `mapstructure:"topic"`
	TLSMode  TLSMode `mapstructure:"tls_mode"`
}

// go-sumtype:decl TLSMode
type TLSMode interface {
	TypeTagged
	isTLSMode()
}

type TLSModeDisabled struct{}
type TLSModeEnabled struct {
	CACertificatePaths []string `mapstructure:"ca_certificate_paths"`
}

// go-sumtype:decl LogbrokerCredentials
type LogbrokerCredentials interface {
	TypeTagged
	isLogbrokerCredentials()
}

type UseCloudCreds struct{}
type LogbrokerOAuthToken struct {
	Token Secret `mapstructure:"token"`
}

func (*LogStdout) isLog()        {}
func (*LogStdout) IsTypeTagged() {}

func (*LogLogbroker) isLog()        {}
func (*LogLogbroker) IsTypeTagged() {}

func (*LogKafka) isLog()        {}
func (*LogKafka) IsTypeTagged() {}

func (*TLSModeDisabled) isTLSMode()    {}
func (*TLSModeDisabled) IsTypeTagged() {}

func (*TLSModeEnabled) isTLSMode()    {}
func (*TLSModeEnabled) IsTypeTagged() {}

func (*UseCloudCreds) isLogbrokerCredentials() {}
func (*UseCloudCreds) IsTypeTagged()           {}

func (*LogbrokerOAuthToken) isLogbrokerCredentials() {}
func (*LogbrokerOAuthToken) IsTypeTagged()           {}

func init() {
	RegisterTypeTagged((*Log)(nil), (*LogStdout)(nil), "stdout", nil)
	RegisterTypeTagged((*Log)(nil), (*LogLogbroker)(nil), "logbroker", nil)
	RegisterTypeTagged((*Log)(nil), (*LogKafka)(nil), "kafka", nil)

	RegisterTypeTagged((*TLSMode)(nil), (*TLSModeDisabled)(nil), "disabled", nil)
	RegisterTypeTagged((*TLSMode)(nil), (*TLSModeEnabled)(nil), "enabled", nil)

	RegisterTypeTagged((*LogbrokerCredentials)(nil), (*UseCloudCreds)(nil), "use_cloud_creds", nil)
	RegisterTypeTagged((*LogbrokerCredentials)(nil), (*LogbrokerOAuthToken)(nil), "oauth_token", nil)
}
