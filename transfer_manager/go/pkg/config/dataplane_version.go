package config

import (
	"os"

	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/buildinfo"
)

// go-sumtype:decl DataplaneVersion
type DataplaneVersion interface {
	TypeTagged
	isDataplaneVersion()
}

type DPVersionLiteral struct {
	Value string `mapstructure:"value"`
}

type DPVersionFromDatabase struct{}

type DPVersionFromEnvironment struct {
	VariableName string `mapstructure:"variable_name"`
}

type DPVersionArcadiaSourceRevision struct{}

type DPVersionComputeInstanceMetadata struct {
	UserDataField string `mapstructure:"user_data_field"`
}

func (*DPVersionLiteral) isDataplaneVersion()      {}
func (*DPVersionFromDatabase) isDataplaneVersion() {}

func (*DPVersionLiteral) IsTypeTagged()                 {}
func (*DPVersionFromDatabase) IsTypeTagged()            {}
func (*DPVersionFromEnvironment) IsTypeTagged()         {}
func (*DPVersionArcadiaSourceRevision) IsTypeTagged()   {}
func (*DPVersionComputeInstanceMetadata) IsTypeTagged() {}

func init() {
	RegisterTypeTagged((*DataplaneVersion)(nil), (*DPVersionLiteral)(nil), "literal", nil)
	RegisterTypeTagged((*DataplaneVersion)(nil), (*DPVersionFromDatabase)(nil), "from_database", nil)
	RegisterTypeTagged((*DataplaneVersion)(nil), (*DPVersionFromEnvironment)(nil), "environment_variable", func(tagged TypeTagged) (interface{}, error) {
		envVarName := tagged.(*DPVersionFromEnvironment).VariableName
		value, ok := os.LookupEnv(envVarName)
		if !ok {
			return nil, xerrors.Errorf("Environment variable %s is not set", envVarName)
		}
		return &DPVersionLiteral{Value: value}, nil
	})
	RegisterTypeTagged((*DataplaneVersion)(nil), (*DPVersionArcadiaSourceRevision)(nil), "arcadia_source_revision", func(tagged TypeTagged) (interface{}, error) {
		return &DPVersionLiteral{Value: buildinfo.ArcadiaVersion()}, nil
	})
	RegisterTypeTagged((*DataplaneVersion)(nil), (*DPVersionComputeInstanceMetadata)(nil), "metadata", func(tagged TypeTagged) (interface{}, error) {
		metadataResolver, err := NewMetadataResolver()
		if err != nil {
			return nil, xerrors.Errorf("Cannot create metadata resolver: %w", err)
		}
		userDataField := tagged.(*DPVersionComputeInstanceMetadata).UserDataField
		dataplaneVersion, err := metadataResolver.ResolveSecret(userDataField)
		if err != nil {
			return nil, xerrors.Errorf("Cannot resolve compute instance metadata field %s: %w", userDataField, err)
		}
		return &DPVersionLiteral{Value: dataplaneVersion}, nil
	})
}
