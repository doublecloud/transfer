package config

import (
	"bytes"
	"errors"
	"flag"
	"io"
	"net/http"
	"os"
	"reflect"
	"sort"
	"strings"

	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/util"
	"github.com/mitchellh/mapstructure"
	"gopkg.in/yaml.v2"
)

type configType string

const (
	EnvControlPlaneConfigFile       = "CONTROL_PLANE_CONFIG_FILE"
	EnvControlPlaneConfigResource   = "CONTROL_PLANE_CONFIG_RESOURCE"
	EnvControlPlaneMetadataResource = "CONTROL_PLANE_METADATA_RESOURCE"
	EnvControlPlaneConfigURL        = "CONTROL_PLANE_CONFIG_URL"
	EnvDataPlaneConfigFile          = "DATA_PLANE_CONFIG_FILE"
	EnvDataPlaneConfigResource      = "DATA_PLANE_CONFIG_RESOURCE"
	EnvDataPlaneMetadataResource    = "DATA_PLANE_METADATA_RESOURCE"
	EnvDataPlaneConfigURL           = "DATA_PLANE_CONFIG_URL"
)

const (
	externalCloudConfigType configType = "external_cloud"
	internalCloudConfigType configType = "internal_cloud"
	awsConfigType           configType = "aws"
)

var (
	ErrConfigNotProvided = errors.New("configuration not provided")

	AllInOneBinary = false
)

type InternalCloudConfig interface {
	YavToken() string
}

type ExternalCloudConfig interface {
	KMSKeyID() string
	KMSDiscoveryEndpoint() string
	CloudCreds() CloudCreds
}

type ConfigBundle struct {
	Common                   interface{}
	Cloud                    interface{}
	ExternalCloud            interface{}
	InternalCloud            interface{}
	AWS                      interface{}
	SecretResolverFactory    SecretResolverFactory
	ProcessedLocalOnlyFields map[string]struct{}
}

type ReaderWithID struct {
	ID     string
	Reader io.Reader
}

func readConfigs(configs []ReaderWithID) (map[interface{}]interface{}, error) {
	configMap := map[interface{}]interface{}{}
	for _, cfg := range configs {
		currentMap := map[interface{}]interface{}{}
		yamlDecoder := yaml.NewDecoder(cfg.Reader)
		if err := yamlDecoder.Decode(&currentMap); err != nil {
			return nil, xerrors.Errorf("Cannot parse %s: %w", cfg.ID, err)
		}
		updateMap(configMap, currentMap)
	}
	return configMap, nil
}

func updateMap(baseMap, newMap map[interface{}]interface{}) {
	for newK, newV := range newMap {
		baseMap[newK] = newV
	}
}

func LoadBundle(configs []ReaderWithID, configBundle ConfigBundle) error {
	var errs util.Errors
	configMap, err := readConfigs(configs)
	if err != nil {
		return xerrors.Errorf("Cannot read configs: %w", err)
	}

	var deferredSecrets []DeferredSecret
	configType, err := decodeCommonConfig(configMap, configBundle, &deferredSecrets)
	if err != nil {
		errs = append(errs, xerrors.Errorf("Cannot decode common configuration: %w", err))
	}
	if err := decodeInstallationSpecificConfig(configType, configMap, configBundle, &deferredSecrets); err != nil {
		errs = append(errs, xerrors.Errorf("Cannot decode %s config: %w", configType, err))
	}
	for key := range configMap {
		errs = append(errs, xerrors.Errorf(`Extra field "%s" found in config`, key))
	}

	sort.Slice(deferredSecrets, func(i, j int) bool {
		_, leftIsYav := deferredSecrets[i].secret.(yavSecret)
		_, rightIsYav := deferredSecrets[j].secret.(yavSecret)
		return leftIsYav && !rightIsYav
	})

	if err := resolveSecrets(deferredSecrets, configBundle); err != nil {
		errs = append(errs, xerrors.Errorf("Cannot resolve secrets: %w", err))
	}
	if len(errs) > 0 {
		return xerrors.Errorf("unable to load config: %w", errs)
	}

	return nil
}

type configLoader = func(reader io.Reader) error

type ConfigSource struct {
	// Only one of these must be non-empty at the same time
	ConfigResource   string
	ConfigFile       string
	MetadataResource string
	ConfigURL        string
}

func checkOptionsCount(configSource *ConfigSource) (ok bool, err error) {
	notEmptyCount := 0
	for _, option := range [...]string{
		configSource.ConfigResource,
		configSource.MetadataResource,
		configSource.ConfigFile,
		configSource.ConfigURL,
	} {
		if len(option) > 0 {
			notEmptyCount++
		}
	}
	if notEmptyCount > 1 {
		return false, xerrors.New("At most one of config-resource, config-metadata-resource, config-file, config-url options must be given")
	}
	return notEmptyCount == 1, nil
}

type recoverableArgParseError struct{ error }

func parseCommandLineOptions(flagSet *flag.FlagSet, configSource *ConfigSource) error {
	usage := flagSet.Usage
	devNull, _ := os.Open(os.DevNull)
	defer devNull.Close()
	flagSet.SetOutput(devNull)
	flagSet.Usage = func() {}
	if err := flagSet.Parse(os.Args[1:]); err != nil {
		if err == flag.ErrHelp {
			usage()
			os.Exit(0)
		}
		//nolint:descriptiveerrors
		return recoverableArgParseError{error: err}
	}

	useCommandLine, err := checkOptionsCount(configSource)
	if err != nil {
		return xerrors.Errorf("Invalid options count: %w", err)
	}
	if !useCommandLine {
		//nolint:descriptiveerrors
		return recoverableArgParseError{error: xerrors.New("No command line options given")}
	}
	return nil
}

func loadOptionsFromEnvironment(envVarMap map[string]*string, configSource *ConfigSource) error {
	for varName, destinationPointer := range envVarMap {
		*destinationPointer = os.Getenv(varName)
	}
	useEnvironmentVariables, err := checkOptionsCount(configSource)
	if err != nil {
		return err
	}
	if !useEnvironmentVariables {
		return xerrors.New("No configuration environment variables are set")
	}
	return nil
}

func NewConfigReader(configResources map[string][]byte, configSource ConfigSource) (io.ReadCloser, error) {
	switch {
	case configSource.ConfigResource != "":
		embeddedYaml, ok := configResources[configSource.ConfigResource]
		if !ok {
			return nil, xerrors.Errorf(`No resource found for name "%s"`, configSource.ConfigResource)
		}
		return io.NopCloser(bytes.NewReader(embeddedYaml)), nil
	case configSource.ConfigFile != "":
		file, err := os.Open(configSource.ConfigFile)
		if err != nil {
			return nil, xerrors.Errorf("Cannot open %s: %w", configSource.ConfigFile, err)
		}
		return file, nil
	case configSource.MetadataResource != "":
		resourceNameTypeErased, err := getUserDataField(configSource.MetadataResource)
		if err != nil {
			return nil, xerrors.Errorf("Cannot get user data field %s: %w", configSource.MetadataResource, err)
		}
		var resourceName string
		var ok bool
		if resourceName, ok = resourceNameTypeErased.(string); !ok {
			return nil, xerrors.Errorf(`Expected field "%s", to be string, not %T`, configSource.MetadataResource, resourceNameTypeErased)
		}
		embeddedYaml, ok := configResources[resourceName]
		if !ok {
			return nil, xerrors.Errorf(`No resource found for name "%s"`, resourceName)
		}
		return io.NopCloser(bytes.NewReader(embeddedYaml)), nil
	case configSource.ConfigURL != "":
		request, err := http.NewRequest("GET", configSource.ConfigURL, nil)
		if err != nil {
			return nil, xerrors.Errorf("Cannot create HTTP request for URL %s: %w", configSource.ConfigURL, err)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			return nil, xerrors.Errorf("Cannot config from %s: %w", configSource.ConfigURL, err)
		}
		return response.Body, nil
	}
	return nil, ErrConfigNotProvided
}

func Load(loader Loader) error {
	flagSet := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	envVarMap := map[string]*string{}
	var configSource ConfigSource
	loader.Setup(flagSet, envVarMap, &configSource)

	if commandLineErr := parseCommandLineOptions(flagSet, &configSource); commandLineErr != nil {
		if _, ok := commandLineErr.(recoverableArgParseError); !ok {
			return xerrors.Errorf("Cannot parse command line arguments: %w", commandLineErr)
		}
		envErr := loadOptionsFromEnvironment(envVarMap, &configSource)
		if envErr != nil {
			return xerrors.Errorf("Cannot load configuration neither from command line arguments (%v) nor from environment (%w)", commandLineErr, envErr)
		}
	}

	configReader, err := NewConfigReader(loader.EmbeddedConfigs(), configSource)
	if err != nil {
		return xerrors.Errorf("Cannot create config reader: %w", err)
	}
	defer configReader.Close()
	return loader.Load(configReader)
}

func decodeCommonConfig(configMap map[interface{}]interface{}, configBundle ConfigBundle, deferredSecrets *[]DeferredSecret) (confType configType, err error) {
	typeField, ok := configMap["type"]
	if !ok {
		return "", xerrors.New(`Field "type" not found`)
	}
	if _, ok := typeField.(string); !ok {
		return "", xerrors.Errorf(`Expected "type" field to be a string, not %T`, typeField)
	}
	delete(configMap, "type")

	common := reflect.New(reflect.TypeOf(configBundle.Common).Elem().Elem())
	if err = decodeMap(configMap, common.Interface(), MakeConfigDecodeHook(deferredSecrets), configBundle.ProcessedLocalOnlyFields); err != nil {
		return "", xerrors.Errorf("Cannot decode config map: %w", err)
	}
	reflect.ValueOf(configBundle.Common).Elem().Set(common)
	return configType(typeField.(string)), nil
}

func decodeInstallationSpecificConfig(configType configType, configMap map[interface{}]interface{}, configBundle ConfigBundle, deferredSecrets *[]DeferredSecret) error {
	switch configType {
	case externalCloudConfigType:
		if err := decodeExternalCloudConfig(configMap, configBundle, deferredSecrets); err != nil {
			return xerrors.Errorf("external cloud config: %w", err)
		}
		return nil
	case internalCloudConfigType:
		if err := decodeInternalCloudConfig(configMap, configBundle, deferredSecrets); err != nil {
			return xerrors.Errorf("internal cloud config: %w", err)
		}
		return nil
	case awsConfigType:
		if err := decodeAWSConfig(configMap, configBundle, deferredSecrets); err != nil {
			return xerrors.Errorf("aws config: %w", err)
		}
		return nil
	}
	return xerrors.Errorf(`Unknown configuration type: "%s"`, configType)
}

func decodeCloudConfig(configMap map[interface{}]interface{}, configBundle ConfigBundle, deferredSecrets *[]DeferredSecret) error {
	cloud := reflect.New(reflect.TypeOf(configBundle.Cloud).Elem().Elem())
	if err := decodeMap(configMap, cloud.Interface(), MakeConfigDecodeHook(deferredSecrets), configBundle.ProcessedLocalOnlyFields); err != nil {
		return err
	}
	reflect.ValueOf(configBundle.Cloud).Elem().Set(cloud)
	return nil
}

func decodeInternalCloudConfig(configMap map[interface{}]interface{}, configBundle ConfigBundle, deferredSecrets *[]DeferredSecret) error {
	if err := decodeCloudConfig(configMap, configBundle, deferredSecrets); err != nil {
		return xerrors.Errorf("Cannot decode cloud config: %w", err)
	}
	internalCloud := reflect.New(reflect.TypeOf(configBundle.InternalCloud).Elem().Elem())
	if err := decodeMap(configMap, internalCloud.Interface(), MakeConfigDecodeHook(deferredSecrets), configBundle.ProcessedLocalOnlyFields); err != nil {
		return xerrors.Errorf("Cannot decode internal cloud config: %w", err)
	}
	reflect.ValueOf(configBundle.InternalCloud).Elem().Set(internalCloud)
	return nil
}

func decodeExternalCloudConfig(configMap map[interface{}]interface{}, configBundle ConfigBundle, deferredSecrets *[]DeferredSecret) error {
	if err := decodeCloudConfig(configMap, configBundle, deferredSecrets); err != nil {
		return xerrors.Errorf("Cannot decode cloud config: %w", err)
	}
	externalCloud := reflect.New(reflect.TypeOf(configBundle.ExternalCloud).Elem().Elem())
	if err := decodeMap(configMap, externalCloud.Interface(), MakeConfigDecodeHook(deferredSecrets), configBundle.ProcessedLocalOnlyFields); err != nil {
		return xerrors.Errorf("Cannot decode external cloud config: %w", err)
	}
	reflect.ValueOf(configBundle.ExternalCloud).Elem().Set(externalCloud)
	return nil
}

func decodeAWSConfig(configMap map[interface{}]interface{}, configBundle ConfigBundle, deferredSecrets *[]DeferredSecret) error {
	aws := reflect.New(reflect.TypeOf(configBundle.AWS).Elem().Elem())
	if err := decodeMap(configMap, aws.Interface(), MakeConfigDecodeHook(deferredSecrets), configBundle.ProcessedLocalOnlyFields); err != nil {
		return err
	}
	reflect.ValueOf(configBundle.AWS).Elem().Set(aws)
	return nil
}

func eraseType(decodeHook mapstructure.DecodeHookFuncValue) interface{} {
	if decodeHook == nil {
		return nil
	}
	return decodeHook
}

func decodeMap(configMap map[interface{}]interface{}, result interface{}, decodeHook mapstructure.DecodeHookFuncValue, processedLocalOnlyFields map[string]struct{}) (err error) {
	var decoderMetadata mapstructure.Metadata
	commonConfigDecoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Result:     result,
		Metadata:   &decoderMetadata,
		DecodeHook: eraseType(decodeHook),
	})
	if err != nil {
		return xerrors.Errorf("Cannot create map decoder: %w", err)
	}
	if err := commonConfigDecoder.Decode(configMap); err != nil {
		return err
	}

	localOnlyFields := map[string]struct{}{}

	type mapstructKey = string
	requiredStructFields := map[mapstructKey]struct{}{}
	resultStructType := reflect.TypeOf(result).Elem()
	for i := 0; i < resultStructType.NumField(); i++ {
		field := resultStructType.Field(i)
		tagValue, ok := field.Tag.Lookup("mapstructure")
		if !ok {
			return xerrors.Errorf(`No mapstructure tag defined for field "%s.%s"`, resultStructType.Name(), field.Name)
		}
		tagItems := strings.Split(tagValue, ",")
		if len(tagItems) < 1 {
			return xerrors.Errorf(`No items given in mapstructure tag for field "%s.%s"`, resultStructType.Name(), field.Name)
		}
		key := tagItems[0]
		if key == "" {
			return xerrors.Errorf(`No mapstructure key defined for field "%s.%s"`, resultStructType.Name(), field.Name)
		} else if len(tagItems) == 2 && tagItems[1] == "local_only" {
			localOnlyFields[key] = struct{}{}
			continue
		}
		requiredStructFields[key] = struct{}{}
	}
	for _, key := range decoderMetadata.Keys {
		delete(configMap, key)
		delete(requiredStructFields, key)
		if processedLocalOnlyFields != nil {
			if _, ok := localOnlyFields[key]; ok {
				processedLocalOnlyFields[key] = struct{}{}
			}
		}
	}
	for key := range requiredStructFields {
		return xerrors.Errorf(`Required field "%s" not found`, key)
	}
	return nil
}

func decodeTypeTaggedValue(ifaceType reflect.Type, value interface{}, deferredSecrets *[]DeferredSecret) (interface{}, error) {
	typeTaggedMap, ok := value.(map[interface{}]interface{})
	if !ok {
		return nil, xerrors.Errorf("Expected map but got %T", value)
	}
	typeTagTypeErased, ok := typeTaggedMap["type"]
	if !ok {
		return nil, xerrors.New("No type tag found")
	}
	delete(typeTaggedMap, "type")

	tag, ok := typeTagTypeErased.(string)
	if !ok {
		return nil, xerrors.Errorf("Type tag must be string, not %T", typeTagTypeErased)
	}

	tagMap, ok := typeTagRegistry[ifaceType]
	if !ok {
		return nil, xerrors.New("No implementations are registered")
	}

	registryEntry, ok := tagMap[typeTag(tag)]
	if !ok {
		return nil, xerrors.Errorf("No implementation registered for type tag %s", tag)
	}

	implValue := reflect.New(registryEntry.implType).Interface()
	if err := decodeMap(typeTaggedMap, implValue, MakeConfigDecodeHook(deferredSecrets), nil); err != nil {
		return nil, xerrors.Errorf("Cannot decode type-tagged map: %w", err)
	}
	for key := range typeTaggedMap {
		return nil, xerrors.Errorf(`Extra field "%s" found`, key)
	}
	if registryEntry.postprocessor != nil {
		postprocessed, err := registryEntry.postprocessor(implValue.(TypeTagged))
		if err != nil {
			return nil, xerrors.Errorf("Postprocessing failed: %w", err)
		}
		implValue = postprocessed
	}
	if !reflect.TypeOf(implValue).Implements(ifaceType) {
		return nil, xerrors.Errorf("Value of type %T does not implement interface type %s", implValue, ifaceType.Name())
	}

	return implValue, nil
}

func MakeConfigDecodeHook(deferredSecrets *[]DeferredSecret) mapstructure.DecodeHookFuncValue {
	return func(sourceValue, destinationValue reflect.Value) (interface{}, error) {
		destinationType := destinationValue.Type()
		if destinationType == reflect.TypeOf(Secret("")) {
			result, err := preprocessSecret(sourceValue, destinationValue, deferredSecrets)
			if err != nil {
				return nil, xerrors.Errorf("unable to preprocess secret, err: %w", err)
			}
			return result, nil
		}
		if destinationType == reflect.TypeOf(EnvVarString("")) || destinationType == reflect.TypeOf(EnvVarInt(0)) {
			result, err := preprocessEnvVar(sourceValue, destinationValue)
			if err != nil {
				return nil, xerrors.Errorf("unable to preprocess env_var, err: %w", err)
			}
			return result, nil
		}
		if isTypeTaggedInterface(destinationType) {
			value, err := decodeTypeTaggedValue(destinationType, sourceValue.Interface(), deferredSecrets)
			if err != nil {
				return nil, xerrors.Errorf("Cannot decode value of type-tagged type %s: %w", destinationType.String(), err)
			}
			return value, nil
		}
		return sourceValue.Interface(), nil
	}
}

func resolveSecrets(deferredSecrets []DeferredSecret, configBundle ConfigBundle) (err error) {
	var yavSecretResolver YavSecretResolver
	var kmsSecretResolver KMSSecretResolver
	var awsSecretResolver AWSSecretResolver
	var metadataSecretResolver MetadataSecretResolver
	var lockboxSecretResolver LockboxSecretResolver
	var awsSecretManagerSecretResolver AWSSecretManagerResolver
	for _, deferredSecret := range deferredSecrets {
		var secretValue string
		switch secret := deferredSecret.secret.(type) {
		case lockboxSecret:
			if lockboxSecretResolver == nil {
				lockboxSecretResolver, err = configBundle.SecretResolverFactory.NewLockboxResolver(configBundle)
				if err != nil {
					return xerrors.Errorf("Cannot create KMS decrypter: %w", err)
				}
			}
			secretValue, err = lockboxSecretResolver.ResolveSecret(secret.SecretID, secret.Key)
			if err != nil {
				return xerrors.Errorf(`Cannot decrypt ciphertext via lockbox: %w; secretID: %s`, err, util.Sample(secret.SecretID, 20))
			}
		case yavSecret:
			if yavSecretResolver == nil {
				if configBundle.InternalCloud == nil {
					return xerrors.Errorf("Yav client is only available in the internal cloud installation")
				}
				yavSecretResolver, err = configBundle.SecretResolverFactory.NewYavResolver(configBundle)
				if err != nil {
					return xerrors.Errorf("Cannot create Yav client: %w", err)
				}
			}
			secretValue, err = yavSecretResolver.ResolveSecret(secret)
			if err != nil {
				return xerrors.Errorf("Cannot resolve Yav secret %s: %w", secret.SecretID, err)
			}
		case awsSecret:
			if awsSecretResolver == nil {
				if configBundle.AWS == nil {
					return xerrors.Errorf("AWS secret resolver is only available in the external cloud installation")
				}
				awsSecretResolver, err = configBundle.SecretResolverFactory.NewAWSResolver(configBundle)
				if err != nil {
					return xerrors.Errorf("Cannot create AWS decrypter: %w", err)
				}
			}
			secretValue, err = awsSecretResolver.ResolveSecret(secret.Ciphertext)
			if err != nil {
				return xerrors.Errorf(`Cannot decrypt ciphertext via AWS: %w; ciphertext: %s`, err, util.Sample(secret.Ciphertext, 20))
			}
		case awsSecretManagerSecret:
			if awsSecretManagerSecretResolver == nil {
				if configBundle.AWS == nil {
					return xerrors.Errorf("AWS secret resolver is only available in the external cloud installation")
				}
				awsSecretManagerSecretResolver, err = configBundle.SecretResolverFactory.NewAWSSecretManagerResolver(configBundle)
				if err != nil {
					return xerrors.Errorf("Cannot create AWS decrypter: %w", err)
				}
			}
			secretValue, err = awsSecretManagerSecretResolver.ResolveSecret(secret.SecretID, secret.Key)
			if err != nil {
				return xerrors.Errorf(`Cannot decrypt ciphertext via AWS: %w; secret id: %s, secret key-name: %s`, err, secret.SecretID, secret.Key)
			}
		case kmsSecret:
			if kmsSecretResolver == nil {
				if configBundle.ExternalCloud == nil {
					return xerrors.Errorf("KMS secret resolver is only available in the external cloud installation")
				}
				kmsSecretResolver, err = configBundle.SecretResolverFactory.NewKMSResolver(configBundle)
				if err != nil {
					return xerrors.Errorf("Cannot create KMS decrypter: %w", err)
				}
			}
			secretValue, err = kmsSecretResolver.ResolveSecret(secret.Ciphertext)
			if err != nil {
				return xerrors.Errorf(`Cannot decrypt ciphertext via KMS: %w; ciphertext: %s`, err, util.Sample(secret.Ciphertext, 20))
			}
		case metadataSecret:
			if metadataSecretResolver == nil {
				metadataSecretResolver, err = configBundle.SecretResolverFactory.NewMetadataResolver(configBundle)
				if err != nil {
					return xerrors.Errorf("Cannot create metadata secret resolver: %w", err)
				}
			}
			secretValue, err = metadataSecretResolver.ResolveSecret(secret.UserDataField)
			if err != nil {
				return xerrors.Errorf(`Cannot get metadata field "%s": %w`, secret.UserDataField, err)
			}
		case envSecret:
			// Should never happen
			panic("All environment variable secrets should have been already resolved")
		}
		if secretValue == "" {
			return xerrors.Errorf("Unknown secret type: %T", deferredSecret.secret)
		}
		deferredSecret.destinationValue.Set(reflect.ValueOf(Secret(secretValue)))
	}
	return nil
}

type yavFactory func() (YavSecretResolver, error)
type kmsFactory func() (KMSSecretResolver, error)
type awsFactory func() (AWSSecretResolver, error)
type awsSecretManagerFactory func() (AWSSecretManagerResolver, error)
type lockboxFactory func() (LockboxSecretResolver, error)

func ResolveSecrets(
	deferredSecrets []DeferredSecret,
	yavFactory yavFactory,
	kmsFactory kmsFactory,
	awsFactory awsFactory,
	lockboxFactory lockboxFactory,
	awsSecretManagerFactory awsSecretManagerFactory,
) error {
	sort.Slice(deferredSecrets, func(i, j int) bool {
		_, leftIsYav := deferredSecrets[i].secret.(yavSecret)
		_, rightIsYav := deferredSecrets[j].secret.(yavSecret)
		return leftIsYav && !rightIsYav
	})
	var yav YavSecretResolver
	var kms KMSSecretResolver
	var lockbox LockboxSecretResolver
	var secretManager AWSSecretManagerResolver
	for _, deferredSecret := range deferredSecrets {
		var value string
		var err error
		switch x := deferredSecret.secret.(type) {
		case yavSecret:
			if yav == nil {
				yav, err = yavFactory()
				if err != nil {
					return xerrors.Errorf("unable to create yav secret resolver: %w", err)
				}
			}
			value, err = yav.ResolveSecret(x)
			if err != nil {
				return xerrors.Errorf("unable to resolve yav secret: %w", err)
			}
		case kmsSecret:
			if kms == nil {
				kms, err = kmsFactory()
				if err != nil {
					return xerrors.Errorf("unable to create kms secret resolver: %w", err)
				}
			}
			value, err = kms.ResolveSecret(x.Ciphertext)
			if err != nil {
				return xerrors.Errorf("unable to resolve kms secret: %w", err)
			}
		case awsSecret:
			if kms == nil {
				kms, err = awsFactory()
				if err != nil {
					return xerrors.Errorf("unable to create kms secret resolver: %w", err)
				}
			}
			value, err = kms.ResolveSecret(x.Ciphertext)
			if err != nil {
				return xerrors.Errorf("unable to resolve kms secret: %w", err)
			}
		case awsSecretManagerSecret:
			if secretManager == nil {
				secretManager, err = awsSecretManagerFactory()
				if err != nil {
					return xerrors.Errorf("unable to create aws secret manager resolver: %w", err)
				}
			}
			value, err = secretManager.ResolveSecret(x.SecretID, x.Key)
			if err != nil {
				return xerrors.Errorf("unable to resolve aws secret manager secret: %w", err)
			}
		case lockboxSecret:
			if lockbox == nil {
				lockbox, err = lockboxFactory()
				if err != nil {
					return xerrors.Errorf("unable to create lockbox secret resolver: %w", err)
				}
			}
			value, err = lockbox.ResolveSecret(x.SecretID, x.Key)
			if err != nil {
				return xerrors.Errorf("unable to resolve lockbox secret: %w", err)
			}
		default:
			return xerrors.Errorf("unsupported secret type: %s", reflect.TypeOf(x).String())
		}
		deferredSecret.destinationValue.Set(reflect.ValueOf(Secret(value)))
	}
	return nil
}
