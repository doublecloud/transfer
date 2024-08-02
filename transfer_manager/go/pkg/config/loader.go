package config

import (
	"flag"
	"io"
)

type Loader interface {
	EmbeddedConfigs() map[string][]byte
	Setup(flagSet *flag.FlagSet, envVarMap map[string]*string, configSource *ConfigSource)
	Load(reader io.Reader) error
}

func SetupControlPlane(flagSet *flag.FlagSet, envVarMap map[string]*string, configSource *ConfigSource) {
	flagSet.StringVar(&configSource.ConfigResource, "controlplane-config-resource", "", "Embedded configuration resource name for controlplane")
	flagSet.StringVar(&configSource.ConfigFile, "controlplane-config-file", "", "Configuration file path for controlplane")
	flagSet.StringVar(&configSource.MetadataResource, "controlplane-config-metadata-resource", "", "Compute instance metadata field containing embedded configuration resource name for controlplane")
	flagSet.StringVar(&configSource.ConfigURL, "controlplane-config-url", "", "Controlplane configuration HTTP URL")
	envVarMap[EnvControlPlaneConfigFile] = &configSource.ConfigFile
	envVarMap[EnvControlPlaneConfigResource] = &configSource.ConfigResource
	envVarMap[EnvControlPlaneMetadataResource] = &configSource.MetadataResource
	envVarMap[EnvControlPlaneConfigURL] = &configSource.ConfigURL
}

func SetupDataPlane(flagSet *flag.FlagSet, envVarMap map[string]*string, configSource *ConfigSource) {
	flagSet.StringVar(&configSource.ConfigResource, "dataplane-config-resource", "", "Embedded configuration resource name for dataplane")
	flagSet.StringVar(&configSource.ConfigFile, "dataplane-config-file", "", "Configuration file path for dataplane")
	flagSet.StringVar(&configSource.MetadataResource, "dataplane-config-metadata-resource", "", "Compute instance metadata field containing embedded configuration resource name for dataplane")
	flagSet.StringVar(&configSource.ConfigURL, "dataplane-config-url", "", "Dataplane configuration HTTP URL")
	envVarMap[EnvDataPlaneConfigFile] = &configSource.ConfigFile
	envVarMap[EnvDataPlaneConfigResource] = &configSource.ConfigResource
	envVarMap[EnvDataPlaneMetadataResource] = &configSource.MetadataResource
	envVarMap[EnvDataPlaneConfigURL] = &configSource.ConfigURL
}
