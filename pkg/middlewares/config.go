package middlewares

type Config struct {
	NoData           bool
	EnableRetries    bool
	ReplicationStage bool
}

type TransferStage string

type ConfigOption func(*Config)

func WithNoData(config *Config)         { config.NoData = true }
func WithEnableRetries(config *Config)  { config.EnableRetries = true }
func AtReplicationStage(config *Config) { config.ReplicationStage = true }

func MakeConfig(options ...ConfigOption) Config {
	config := &Config{
		NoData:           false,
		EnableRetries:    false,
		ReplicationStage: false,
	}
	for _, opt := range options {
		opt(config)
	}
	return *config
}
