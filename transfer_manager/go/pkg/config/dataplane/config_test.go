package dpconfig

import (
	"bytes"
	_ "embed"
	"os"
	"testing"

	"github.com/doublecloud/tross/transfer_manager/go/pkg/config"
	_ "github.com/doublecloud/tross/transfer_manager/go/pkg/metering/writer/kafka"
	_ "github.com/doublecloud/tross/transfer_manager/go/pkg/metering/writer/kinesis"
	_ "github.com/doublecloud/tross/transfer_manager/go/pkg/metering/writer/logbroker"
	_ "github.com/doublecloud/tross/transfer_manager/go/pkg/metering/writer/stdout"
	"github.com/stretchr/testify/require"
)

var (
	//go:embed devconfigs/external_preprod.yaml
	devExternalPreprodConfig []byte

	//go:embed devconfigs/external_prod.yaml
	devExternalProdConfig []byte

	//go:embed devconfigs/internal.yaml
	devInternalConfig []byte

	//go:embed devconfigs/internal_minimal.yaml
	devInternalMinimalConfig []byte

	//go:embed devconfigs/ovandriyanov/external/preprod.yaml
	devOvandriyanovExternalPreprod []byte

	//go:embed devconfigs/ovandriyanov/internal/prod.yaml
	devOvandriyanovInternalProd []byte

	//go:embed devconfigs/ovandriyanov/internal/testing.yaml
	devOvandriyanovInternalTesting []byte

	//go:embed devconfigs/dc_local.yaml
	dcLocal []byte

	//go:embed devconfigs/dc_local_proxy.yaml
	dcLocalProxy []byte

	devConfigs = map[string][]byte{
		"external_preprod.yaml":              devExternalPreprodConfig,
		"external_prod.yaml":                 devExternalProdConfig,
		"internal.yaml":                      devInternalConfig,
		"internal_minimal.yaml":              devInternalMinimalConfig,
		"ovandriyanov/external/preprod.yaml": devOvandriyanovExternalPreprod,
		"ovandriyanov/internal/prod.yaml":    devOvandriyanovInternalProd,
		"ovandriyanov/internal/testing.yaml": devOvandriyanovInternalTesting,
		"dc_local":                           dcLocal,
		"dc_local_proxy":                     dcLocalProxy,
	}

	internalVariables = [...]string{
		"YT_SECURE_VAULT_dp_token",
		"YT_SECURE_VAULT_log_lb_token",
		"YT_SECURE_VAULT_robot_logfeller_nirvactor_token",
		"YT_SECURE_VAULT_robot_logfeller_logbroker_token",
		"YT_SECURE_VAULT_robot_logfeller_describer_logbroker_token",
		"YT_SECURE_VAULT_robot_logfeller_yt_token",
		"YT_SECURE_VAULT_solomon_token",
		"YT_SECURE_VAULT_ydb_token",
		"YT_SECURE_VAULT_robot_yt_token",
		"YT_SECURE_VAULT_robot_mdb_token",
	}
)

func setInternalVariables() {
	for _, varName := range internalVariables {
		_ = os.Setenv(varName, "x")
	}
}

func unsetInternalVariables() {
	for _, varName := range internalVariables {
		_ = os.Unsetenv(varName)
	}
}

func clearConfigs() {
	Common = nil
	Cloud = nil
	InternalCloud = nil
	ExternalCloud = nil
	AWS = nil
}

func TestInstallationsConfigsSanity(t *testing.T) {
	for configName, configContent := range EmbeddedConfigs {
		t.Run(configName, func(t *testing.T) {
			configReader := bytes.NewReader(configContent)
			if configName == "internal_prod.yaml" || configName == "internal_testing.yaml" {
				setInternalVariables()
			}
			processedLocalOnlyFields, err := LoadF(configReader, config.MockSecretResolverFactory{T: t})
			unsetInternalVariables()
			require.NoErrorf(t, err, "Error occurred while checking sanity of installations/%s config file", configName)
			require.Emptyf(t, processedLocalOnlyFields, "No local-only fields must be specified in application configuration")
			clearConfigs()
		})
	}
	_ = os.Setenv("YAV_TOKEN", "x")
	for configName, configContent := range devConfigs {
		t.Run("devconfig/"+configName, func(t *testing.T) {
			if configName == "ovandriyanov/internal/prod.yaml" || configName == "ovandriyanov/internal/testing.yaml" {
				setInternalVariables()
			}
			if configName == "internal_minimal.yaml" {
				_ = os.Setenv("YC_TOKEN", "x")
			}
			_, err := LoadF(bytes.NewReader(configContent), config.MockSecretResolverFactory{T: t})
			unsetInternalVariables()
			_ = os.Unsetenv("YC_TOKEN")
			require.NoErrorf(t, err, "Error occurred while checking sanity of devconfigs/%s config file", configName)
			clearConfigs()
		})
	}
	_ = os.Unsetenv("YAV_TOKEN")
}
