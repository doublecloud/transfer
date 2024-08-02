package cpconfig

import (
	"flag"
	"io"

	"github.com/doublecloud/tross/transfer_manager/go/pkg/config"
)

type Loader struct{}

func (l *Loader) EmbeddedConfigs() map[string][]byte {
	return EmbeddedConfigs
}

func (l *Loader) Setup(flagSet *flag.FlagSet, envVarMap map[string]*string, configSource *config.ConfigSource) {
	config.SetupControlPlane(flagSet, envVarMap, configSource)
	if config.AllInOneBinary {
		// Allow data plane flags to be passed on the command line
		var dummyConfigSource config.ConfigSource
		config.SetupDataPlane(flagSet, envVarMap, &dummyConfigSource)
	}
}

func (l *Loader) Load(reader io.Reader) error {
	_, err := LoadF(reader, SecretResolverFactory{})
	return err
}
