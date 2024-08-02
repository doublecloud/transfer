package config

import (
	"os"
	"reflect"
	"strconv"

	"github.com/doublecloud/tross/library/go/core/xerrors"
)

type EnvVarString string
type EnvVarInt int

type envVarType string

const (
	environmentVarType envVarType = "environment_variable"
)

type envVar struct {
	VariableName string `mapstructure:"variable_name"`
}

func (envVar) isEnvVar() {}

func preprocessEnvVar(sourceValue, destinationValue reflect.Value) (interface{}, error) {
	if sourceValue.Type() == reflect.TypeOf("") {
		// EnvVarString given as a literal string value, return as-is
		return sourceValue.Interface(), nil
	} else if sourceValue.Type() == reflect.TypeOf(0) {
		// EnvVarInt given as a literal int value, return as-is
		return sourceValue.Interface(), nil
	} else if sourceValue.Type().Kind() != reflect.Map {
		return nil, xerrors.Errorf("EnvVar field must be either a literal string/int or a map, not %s", sourceValue.Type().Kind())
	}
	envVarMap := sourceValue.Interface().(map[interface{}]interface{})
	envVarTypeInterface, ok := envVarMap["type"]
	if !ok {
		return nil, xerrors.New(`No "type" field found for env var`)
	}
	envVarTypeString, ok := envVarTypeInterface.(string)
	if !ok {
		return nil, xerrors.Errorf(`Expected value for field "type" to be string, not %T`, envVarTypeInterface)
	}
	delete(envVarMap, "type")

	resolvedEnvVar, err := decodeEnvVar(envVarType(envVarTypeString), envVarMap)
	if err != nil {
		return nil, xerrors.Errorf(`Cannot decode "%s" env-var: %w`, envVarTypeString, err)
	}
	for key := range envVarMap {
		return nil, xerrors.Errorf(`Extra field "%s" found in env-var of type "%s"`, key, envVarTypeString)
	}

	destinationType := destinationValue.Type()
	if destinationType == reflect.TypeOf(EnvVarString("")) {
		return resolvedEnvVar, nil
	}
	if destinationType == reflect.TypeOf(EnvVarInt(0)) {
		result, err := strconv.Atoi(resolvedEnvVar)
		if err != nil {
			return nil, xerrors.Errorf("unable to convert envVarIntType into int, val:%s, err:%w", resolvedEnvVar, err)
		}
		return result, nil
	}

	return resolvedEnvVar, nil
}

func decodeEnvVar(
	envVarType envVarType,
	envVarSpecificData map[interface{}]interface{},
) (string, error) {
	switch envVarType {
	case environmentVarType:
		var envVar envVar
		if err := decodeMap(envVarSpecificData, &envVar, nil, nil); err != nil {
			return "", xerrors.Errorf("env: %w", err)
		}
		varValue, ok := os.LookupEnv(envVar.VariableName)
		if !ok {
			return "", xerrors.Errorf(`Environment variable "%s" is not set`, envVar.VariableName)
		}
		return varValue, nil
	}
	return "", xerrors.New("Unknown env_var type")
}
