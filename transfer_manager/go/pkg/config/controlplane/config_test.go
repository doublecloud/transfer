package cpconfig

import (
	"bytes"
	_ "embed"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/doublecloud/tross/transfer_manager/go/pkg/config"
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

	devConfigs = map[string][]byte{
		"external_preprod.yaml":              devExternalPreprodConfig,
		"external_prod.yaml":                 devExternalProdConfig,
		"internal.yaml":                      devInternalConfig,
		"internal_minimal.yaml":              devInternalMinimalConfig,
		"ovandriyanov/external/preprod.yaml": devOvandriyanovExternalPreprod,
		"ovandriyanov/internal/prod.yaml":    devOvandriyanovInternalProd,
		"ovandriyanov/internal/testing.yaml": devOvandriyanovInternalTesting,
	}
)

func clearConfigs() {
	Common = nil
	Cloud = nil
	InternalCloud = nil
	ExternalCloud = nil
	AWS = nil
}

func init() {
	config.UserData = map[string]interface{}{
		"dataplane_image_id": "x",
	}
}

func TestInstallationsConfigsSanity(t *testing.T) {
	_ = os.Setenv("DATA_PLANE_K8S_IMAGE", "x")
	for configName, configContent := range EmbeddedConfigs {
		t.Run(configName, func(t *testing.T) {
			if strings.HasPrefix(configName, "parts/") {
				t.Skip()
				return
			}
			configReader := bytes.NewReader(configContent)
			if configName == "internal_prod.yaml" || configName == "internal_testing.yaml" {
				_ = os.Setenv("YAV_TOKEN", "x")
			}
			processedLocalOnlyFields, err := LoadF(configReader, config.MockSecretResolverFactory{T: t})
			_ = os.Unsetenv("YAV_TOKEN")
			require.NoErrorf(t, err, "Error occurred while checking sanity of installations/%s config file", configName)
			require.Emptyf(t, processedLocalOnlyFields, "No local-only fields must be specified in application configuration")
			clearConfigs()
		})
	}
	_ = os.Setenv("YAV_TOKEN", "x")
	// For devconfigs/internal_minimal
	_ = os.Setenv("YC_TOKEN", "x")
	defer os.Unsetenv("YC_TOKEN")
	_ = os.Setenv("CP_DB_PASSWORD", "x")
	defer os.Unsetenv("CP_DB_PASSWORD")
	for configName, configContent := range devConfigs {
		t.Run("devconfig/"+configName, func(t *testing.T) {
			_, err := LoadF(bytes.NewReader(configContent), config.MockSecretResolverFactory{T: t})
			require.NoErrorf(t, err, "Error occurred while checking sanity of devconfigs/%s config file", configName)
			clearConfigs()
		})
	}
	_ = os.Unsetenv("YAV_TOKEN")
	_ = os.Unsetenv("DATA_PLANE_K8S_IMAGE")
}

func TestComputeCertificate(t *testing.T) {
	certificate, err := config.ParseCertificate("-----BEGIN CERTIFICATE-----\nMIIC4TCCAcmgAwIBAgIUP0zcGO1MeRwze8VdSMEt/OdBXoIwDQYJKoZIhvcNAQEL\nBQAwADAeFw0yMzA2MDcwNjU4MTBaFw0zMzA2MDQwNjU4MTBaMAAwggEiMA0GCSqG\nSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDw6TvAvrbJvY4tzIIuDnLEVfRUW4BZJD3y\nK8fyyxXrYDvC69RKCKk9+TQhnUOLhZNlDST4HFfSPlakOjXUduyJE5M1EmoLAstN\n81aP3TejseDavxmaNijXRsa9E731T5H+zo44PgAHfQJmiD7rtcr+QOIosKUB2dwp\nF2acp9hLKd389BfNctziG0Oxq7hlISTDBnhzBg7eKuqWtShjVW5RqQvp3bARfUPa\nRWdYjmZvR+AnmozV1SGnpAnatzhnF6tNAb5XSEw49tumsX1D4A11J6mtrafO6bsP\nwdIPwy9W15iCszUNlFcdBaZhESc34VbyCyLMvA5T0Uj1FJHz1RFlAgMBAAGjUzBR\nMB0GA1UdDgQWBBQq0z6Vcmjcn8wnRTwKGSm5YGas9TAfBgNVHSMEGDAWgBQq0z6V\ncmjcn8wnRTwKGSm5YGas9TAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUA\nA4IBAQBplippQ/Pxn7AkuwOTSwSTeJ7S+rMSb6iSL9chNHetanft0Ikr5BDsSrd6\nTeHV0sEMilDIjX0EjSNHwYtYrDPk6cGjkzDTYb6/U10c5Xhwi0g7/lMH/RPihPz5\nco80VEqXWlgfgHuE7/cAiTJ61PiFD9oI494bQcIISQNDfbUUiYfn32+8nK20rn8C\nw7PbGoIv6zz6A0c6DJT7yXJF5sAHgX4M03Oi9edzQ077ZOboXSuUKe4VfHIpjTjZ\n0sM/NbG5BFstyetVc3FZOGWGukTRb0C0GSASOm6hCyh5ctmpwlS4menc/OAx9BYO\nr9ZBjEa0oLFVV0pP5Tj4Gf1DDpuJ\n-----END CERTIFICATE-----\n")
	require.NoError(t, err)
	require.True(t, certificate.NotBefore.Before(time.Now()))
	twoMonths := 2 * 30 * 24 * time.Hour
	require.True(t, certificate.NotAfter.After(time.Now().Add(twoMonths)))
}
