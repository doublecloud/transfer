package config

import (
	"bytes"
	"io"

	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/mitchellh/mapstructure"
	"gopkg.in/yaml.v2"
)

type includeType string

const (
	FileType             includeType = "file"
	ResourceType         includeType = "resource"
	MetadataResourceType includeType = "metadata_resource"
	URLType              includeType = "url"
)

type Include struct {
	Type includeType

	Path string
	Name string
	URL  string
}

type ReadCloserWithID struct {
	ID         string
	ReadCloser io.ReadCloser
}

func validateIncludeConfig(include Include, metadata mapstructure.Metadata) error {
	switch include.Type {
	case FileType:
		if include.Path == "" || len(metadata.Keys) != 2 {
			return xerrors.Errorf(`Expect "path"" field only for include type %s`, include.Type)
		}
	case ResourceType, MetadataResourceType:
		if include.Name == "" || len(metadata.Keys) != 2 {
			return xerrors.Errorf(`Expect "name"" field only for include type %s`, include.Type)
		}
	case URLType:
		if include.URL == "" || len(metadata.Keys) != 2 {
			return xerrors.Errorf(`Expect "url"" field only for include type %s`, include.Type)
		}
	}
	return nil
}

func handleInclude(includeRaw interface{}) (ConfigSource, Include, error) {
	var include Include
	var decodeMetadata mapstructure.Metadata
	includesDecoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Result:      &include,
		Metadata:    &decodeMetadata,
		ErrorUnused: true,
	})
	if err != nil {
		return ConfigSource{}, Include{}, xerrors.Errorf(`Cannot create includes decoder: %w`, err)
	}
	if err := includesDecoder.Decode(includeRaw); err != nil {
		return ConfigSource{}, Include{}, xerrors.Errorf(`Cannot decode the includes correctly: %w`, err)
	}

	if err := validateIncludeConfig(include, decodeMetadata); err != nil {
		return ConfigSource{}, Include{}, xerrors.Errorf("Cannot parse include: %w", err)
	}

	var configSource ConfigSource
	switch include.Type {
	case FileType:
		configSource.ConfigFile = include.Path
	case ResourceType:
		configSource.ConfigResource = include.Name
	case MetadataResourceType:
		configSource.MetadataResource = include.Name
	case URLType:
		configSource.ConfigURL = include.URL
	}

	return configSource, include, nil
}

func PreprocessIncludes(configReader io.Reader, configResources map[string][]byte) ([]ReadCloserWithID, error) {
	baseConfigReader := ReaderWithID{"base high level config", configReader}
	configMap, err := readConfigs([]ReaderWithID{baseConfigReader})
	if err != nil {
		return nil, xerrors.Errorf("Cannot read configs: %w", err)
	}

	lastReader := new(bytes.Buffer)
	var resultConfigReadClosers []ReadCloserWithID

	addLastReader := func() error {
		yamlEncoder := yaml.NewEncoder(lastReader)
		if err := yamlEncoder.Encode(configMap); err != nil {
			return xerrors.Errorf("Cannot create yaml encoder: %w", err)
		}
		resultConfigReadClosers = append(resultConfigReadClosers, ReadCloserWithID{baseConfigReader.ID, io.NopCloser(lastReader)})
		return nil
	}

	includesRawTypeErased, ok := configMap["include_configs"]
	if !ok {
		err = addLastReader()
		if err != nil {
			return nil, xerrors.Errorf("Cannot add last item to configs: %w", err)
		}
		return resultConfigReadClosers, nil
	}

	includesRaw, ok := includesRawTypeErased.([]interface{})
	if !ok {
		return nil, xerrors.New(`"include_configs" field must be an array`)
	}

	for _, includeRaw := range includesRaw {
		configSource, include, err := handleInclude(includeRaw)
		if err != nil {
			return nil, xerrors.Errorf("Cannot handle include: %w", err)
		}
		newReader, err := NewConfigReader(configResources, configSource)
		if err != nil {
			return nil, xerrors.Errorf("Cannot get include type for %s include with type %s", include.Path, include.Type)
		}
		resultConfigReadClosers = append(resultConfigReadClosers, ReadCloserWithID{include.Path, newReader})
	}
	delete(configMap, "include_configs")

	err = addLastReader()
	if err != nil {
		return nil, xerrors.Errorf("Cannot add last item to configs: %w", err)
	}
	return resultConfigReadClosers, nil
}
