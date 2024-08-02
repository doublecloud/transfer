package config

import (
	"os"
	"reflect"

	"github.com/doublecloud/tross/library/go/core/xerrors"
)

type Secret string

type DeferredSecret struct {
	secret           secret
	destinationValue reflect.Value
}

type secretType string

const (
	yavSecretType              secretType = "yav"
	awsSecretType              secretType = "aws"
	awsSecretManagerSecretType secretType = "aws_secret_manager"
	kmsSecretType              secretType = "kms"
	environmentSecretType      secretType = "environment_variable"
	metadataSecretType         secretType = "metadata"
	lockboxSecretType          secretType = "lockbox"
	emptySecretType            secretType = "empty"
)

// go-sumtype:decl secret
type secret interface {
	isSecret()
}

var (
	_ secret = (*yavSecret)(nil)
	_ secret = (*kmsSecret)(nil)
	_ secret = (*envSecret)(nil)
	_ secret = (*awsSecret)(nil)
	_ secret = (*awsSecretManagerSecret)(nil)
	_ secret = (*metadataSecret)(nil)
	_ secret = (*lockboxSecret)(nil)
	_ secret = (*emptySecret)(nil)
)

type yavSecret struct {
	SecretID string `mapstructure:"secret_id"`
	Key      string `mapstructure:"key"`
	Version  string `mapstructure:"version"`
}

type awsSecret struct {
	Ciphertext string `mapstructure:"ciphertext"`
}

type awsSecretManagerSecret struct {
	SecretID string `mapstructure:"secret_id"`
	Key      string `mapstructure:"key"`
}

type kmsSecret struct {
	Ciphertext string `mapstructure:"ciphertext"`
}

type envSecret struct {
	VariableName string `mapstructure:"variable_name"`
}

type metadataSecret struct {
	UserDataField string `mapstructure:"user_data_field"`
}

type lockboxSecret struct {
	SecretID string `mapstructure:"secret_id"`
	Key      string `mapstructure:"key"`
}

type emptySecret struct{}

func (yavSecret) isSecret()              {}
func (kmsSecret) isSecret()              {}
func (envSecret) isSecret()              {}
func (awsSecret) isSecret()              {}
func (awsSecretManagerSecret) isSecret() {}
func (metadataSecret) isSecret()         {}
func (lockboxSecret) isSecret()          {}
func (emptySecret) isSecret()            {}

func preprocessSecret(sourceValue, destinationValue reflect.Value, deferredSecrets *[]DeferredSecret) (interface{}, error) {
	if sourceValue.Type() == reflect.TypeOf("") {
		// Secret given as a literal string value, return as-is
		return sourceValue.Interface(), nil
	} else if sourceValue.Type().Kind() != reflect.Map {
		return nil, xerrors.Errorf("Secret field must be either a literal string or a map, not %s", sourceValue.Type().Kind())
	}
	secretMap := sourceValue.Interface().(map[interface{}]interface{})
	secretTypeInterface, ok := secretMap["type"]
	if !ok {
		return nil, xerrors.New(`No "type" field found for secret`)
	}
	secretTypeString, ok := secretTypeInterface.(string)
	if !ok {
		return nil, xerrors.Errorf(`Expected value for field "type" to be string, not %T`, secretTypeInterface)
	}
	delete(secretMap, "type")

	resolvedSecret, err := decodeSecret(secretType(secretTypeString), secretMap, destinationValue, deferredSecrets)
	if err != nil {
		return nil, xerrors.Errorf(`Cannot decode "%s" secret: %w`, secretTypeString, err)
	}
	for key := range secretMap {
		return nil, xerrors.Errorf(`Extra field "%s" found in secret of type "%s"`, key, secretTypeString)
	}
	return resolvedSecret, nil
}

func decodeSecret(
	secretType secretType,
	secretSpecificData map[interface{}]interface{},
	destinationValue reflect.Value,
	deferredSecrets *[]DeferredSecret,
) (string, error) {
	switch secretType {
	case lockboxSecretType:
		var secret lockboxSecret
		if err := decodeMap(secretSpecificData, &secret, nil, nil); err != nil {
			return "", xerrors.Errorf("lockbox: %w", err)
		}
		*deferredSecrets = append(*deferredSecrets, DeferredSecret{secret: secret, destinationValue: destinationValue})
		return "", nil
	case yavSecretType:
		var secret yavSecret
		if err := decodeMap(secretSpecificData, &secret, nil, nil); err != nil {
			return "", xerrors.Errorf("yav: %w", err)
		}
		*deferredSecrets = append(*deferredSecrets, DeferredSecret{secret: secret, destinationValue: destinationValue})
		return "", nil
	case kmsSecretType:
		var secret kmsSecret
		if err := decodeMap(secretSpecificData, &secret, nil, nil); err != nil {
			return "", xerrors.Errorf("kms: %w", err)
		}
		*deferredSecrets = append(*deferredSecrets, DeferredSecret{secret: secret, destinationValue: destinationValue})
		return "", nil
	case environmentSecretType:
		var secret envSecret
		if err := decodeMap(secretSpecificData, &secret, nil, nil); err != nil {
			return "", xerrors.Errorf("env: %w", err)
		}
		varValue, ok := os.LookupEnv(secret.VariableName)
		if !ok {
			return "", xerrors.Errorf(`Environment variable "%s" is not set`, secret.VariableName)
		}
		return varValue, nil
	case metadataSecretType:
		var secret metadataSecret
		if err := decodeMap(secretSpecificData, &secret, nil, nil); err != nil {
			return "", xerrors.Errorf("metadata: %w", err)
		}
		*deferredSecrets = append(*deferredSecrets, DeferredSecret{secret: secret, destinationValue: destinationValue})
		return "", nil
	case awsSecretType:
		var secret awsSecret
		if err := decodeMap(secretSpecificData, &secret, nil, nil); err != nil {
			return "", xerrors.Errorf("aws: %w", err)
		}
		*deferredSecrets = append(*deferredSecrets, DeferredSecret{secret: secret, destinationValue: destinationValue})
		return "", nil
	case awsSecretManagerSecretType:
		var secret awsSecretManagerSecret
		if err := decodeMap(secretSpecificData, &secret, nil, nil); err != nil {
			return "", xerrors.Errorf("aws: %w", err)
		}
		*deferredSecrets = append(*deferredSecrets, DeferredSecret{secret: secret, destinationValue: destinationValue})
		return "", nil
	case emptySecretType:
		return "", nil
	}
	return "", xerrors.New("Unknown secret type")
}
