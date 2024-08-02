package dpconfig

import (
	"bufio"
	"crypto/tls"
	_ "embed"
	"fmt"
	"io"
	"strings"

	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/library/go/slices"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/config"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/xtls"
)

var (
	Common *CommonConfig

	// Not nil when either Internal != nil or External != nil
	Cloud *CloudConfig

	// Exactly one of these is not nil
	InternalCloud *InternalCloudConfig
	ExternalCloud *ExternalCloudConfig
	AWS           *AWSConfig
)

var (
	//go:embed installations/aws_preprod.yaml
	awsPreprodYaml []byte

	//go:embed installations/aws_prod.yaml
	awsProdYaml []byte

	//go:embed installations/external_preprod.yaml
	externalPreprodYaml []byte

	//go:embed installations/external_prod.yaml
	externalProdYaml []byte

	//go:embed installations/external_kz.yaml
	externalKzYaml []byte

	//go:embed installations/israel.yaml
	israelYaml []byte

	//go:embed installations/internal_prod.yaml
	internalProdYaml []byte

	//go:embed installations/internal_testing.yaml
	internalTestingYaml []byte

	//go:embed installations/nemax.yaml
	nemaxYaml []byte

	//go:embed devconfigs/dc_local.yaml
	dcLocalYaml []byte

	EmbeddedConfigs = map[string][]byte{
		"aws_preprod.yaml":      awsPreprodYaml,
		"aws_prod.yaml":         awsProdYaml,
		"external_preprod.yaml": externalPreprodYaml,
		"external_prod.yaml":    externalProdYaml,
		"external_kz.yaml":      externalKzYaml,
		"internal_prod.yaml":    internalProdYaml,
		"internal_testing.yaml": internalTestingYaml,
		"israel.yaml":           israelYaml,
		"nemax.yaml":            nemaxYaml,
		"dc_local.yaml":         dcLocalYaml,
	}
)

type CommonConfig struct {
	ControlPlaneAPI  ControlPlaneAPI `mapstructure:"control_plane_api"`
	Log              config.Log      `mapstructure:"log"`
	Profiler         config.Profiler `mapstructure:"profiler"`
	PgDumpCommand    []string        `mapstructure:"pg_dump_command"`
	BypassRunnerMain bool            `mapstructure:"bypass_runner_main,local_only"`
	RootCACertPaths  []string        `mapstructure:"root_cert_paths"`

	Environment     config.Environment `mapstructure:"environment"`
	Metering        config.Metering    `mapstructure:"metering"`
	ClickhouseLocal string             `mapstructure:"clickhouse_local"`
}

type CloudConfig struct {
	CloudCreds       config.CloudCreds `mapstructure:"cloud_creds"`
	MDBAPIURLBase    string            `mapstructure:"mdb_api_url_base"`
	YavToken         config.Secret     `mapstructure:"yav_token,local_only"`
	CloudAPIEndpoint string            `mapstructure:"cloud_api_endpoint"`
}

type InternalCloudConfig struct {
	SolomonToken                       config.Secret `mapstructure:"solomon_token"`
	YDBToken                           config.Secret `mapstructure:"ydb_token"`
	YtToken                            config.Secret `mapstructure:"yt_token"`
	DataplaneToken                     config.Secret `mapstructure:"dataplane_token"`
	LogfellerYtToken                   config.Secret `mapstructure:"logfeller_yt_token"`
	LogfellerNirvactorToken            config.Secret `mapstructure:"logfeller_nirvactor_token"`
	LogfellerLogbrokerToken            config.Secret `mapstructure:"logfeller_logbroker_token"`
	LogfellerLfDescriberLogbrokerToken config.Secret `mapstructure:"logfeller_lf_describer_logbroker_token"`
	LogbrokerToken                     config.Secret `mapstructure:"logbroker_token"`
	MDBToken                           config.Secret `mapstructure:"mdb_token"`
	IAMAPIURLBase                      string        `mapstructure:"iam_api_url_base"`
	SolomonCluster                     string        `mapstructure:"solomon_cluster"`
	YtProxy                            string        `mapstructure:"yt_proxy"`
	YDBInstance                        string        `mapstructure:"ydb_instance"`
	YDBTrackerDatabase                 string        `mapstructure:"ydb_tracker_database"`
	UDFDir                             string        `mapstructure:"udf_dir"`
	HostnameReplacePolicyDisabled      bool          `mapstructure:"hostname_replace_policy_disabled,local_only"`
}

type ExternalCloudConfig struct {
	ServiceCloudID        string         `mapstructure:"service_cloud_id"`
	ServiceFolderID       string         `mapstructure:"service_folder_id"`
	SolomonURL            string         `mapstructure:"solomon_url"`
	CloudFunctionsBaseURL string         `mapstructure:"cloud_functions_base_url"`
	KMSKeyID              string         `mapstructure:"kms_key_id"`
	KMSDiscoveryEndpoint  string         `mapstructure:"kms_discovery_endpoint"`
	YDSServerlessEndpoint string         `mapstructure:"yds_serverless_endpoint"`
	Metrika               config.Metrika `mapstructure:"metrika"`
}

type AWSConfig struct {
	// Profile allows to specify what aws-cli profile should be use for authentication.
	// 	Useful for local development when you have multiple accounts and profiles.
	Profile string `mapstructure:"profile,local_only"`

	CloudCreds config.CloudCreds `mapstructure:"cloud_creds"`
	KeyID      string            `mapstructure:"key_id"`
	IAM        IAMApis           `mapstructure:"iam"`
	MDBService string            `mapstructure:"mdb_service"`

	// AssumeRole is role to assume for our dataplane for interaction with our CP resources if there is no chain of assumes.
	// default is arn:aws:iam::{{.CPAccountID}}:role/dataplane-assume-role
	AssumeRole  config.RemoteValue `mapstructure:"assume_role_target"`
	CPAccountID string             `mapstructure:"cp_account_id"`

	// AssumeChainValues allows us to specify chain of IAM assumes to get correct role with required permissions
	// 	usually it lead to a role with access to our KMS/S3 bucket, contains multiline string where each line is a chain step
	// 	as example:
	// 		arn:aws:iam::{cpconfig.AWS.DPAccountID}:role/DoubleCloud/clusters/dataplane-node-transfer-for-{BYOA_USER_ACCOUNT_ID}
	// 		arn:aws:iam::{cpconfig.AWS.DPAccountID}:role/data-transfer-mdb-dp
	AssumeChainValues config.RemoteValue `mapstructure:"assume_role_target_chain"`

	// AWS-managed Prometheus Service (i.e. APS)
	AMPRegion string `mapstructure:"amp_region"`
	AMPURL    string `mapstructure:"amp_url"`

	// Container registry service for publicly available Docker images
	ContainerRegistryService string `mapstructure:"public_container_registry_service"`
	// Tag of the DBT image
	DBTImageTag string `mapstructure:"dbt_image_tag"`
}

func (c AWSConfig) ResolveAssumeRole() string {
	if c.AssumeRole.String() == "" {
		return fmt.Sprintf("arn:aws:iam::%s:role/dataplane-assume-role", c.CPAccountID)
	}
	return c.AssumeRole.String()
}

func (c AWSConfig) AssumeChain() []string {
	var lines []string
	sc := bufio.NewScanner(strings.NewReader(c.AssumeChainValues.String()))
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	return slices.Filter(lines, func(t string) bool {
		return t != ""
	})
}

func (c CommonConfig) TLSConfig() (*tls.Config, error) {
	return xtls.FromPath(c.RootCACertPaths)
}

type IAMApis struct {
	AccessService  string `mapstructure:"access_service"`
	SessionService string `mapstructure:"session_service"`
	OrgService     string `mapstructure:"org_service"`
	TokenService   string `mapstructure:"token_service"`
}

type ControlPlaneAPI struct {
	Endpoint  string `mapstructure:"endpoint"`
	Plaintext bool   `mapstructure:"plaintext"`
}

func IsDataPlane() bool {
	return Common != nil
}

func IsCloud() bool {
	return Cloud != nil
}

func IsExternalCloud() bool {
	return ExternalCloud != nil
}

func IsInternalCloud() bool {
	return InternalCloud != nil
}

func IsAWS() bool {
	return AWS != nil
}

func LoadF(reader io.Reader, secretResolverFactory config.SecretResolverFactory) (processedLocalOnlyFields map[string]struct{}, err error) {
	configReadClosers, err := config.PreprocessIncludes(reader, EmbeddedConfigs)
	if err != nil {
		return nil, xerrors.Errorf("Cannot preprocess include configs: %w", err)
	}

	defer func() {
		for _, configReadCloser := range configReadClosers {
			_ = configReadCloser.ReadCloser.Close()
		}
	}()

	configReaders := make([]config.ReaderWithID, len(configReadClosers))
	for i, configReadCloser := range configReadClosers {
		configReaders[i] = config.ReaderWithID{ID: configReadCloser.ID, Reader: configReadCloser.ReadCloser}
	}

	processedLocalOnlyFields = map[string]struct{}{}
	err = config.LoadBundle(configReaders, config.ConfigBundle{
		Common:                   &Common,
		Cloud:                    &Cloud,
		ExternalCloud:            &ExternalCloud,
		InternalCloud:            &InternalCloud,
		AWS:                      &AWS,
		SecretResolverFactory:    secretResolverFactory,
		ProcessedLocalOnlyFields: processedLocalOnlyFields,
	})
	return processedLocalOnlyFields, err
}

func Load() error {
	return config.Load(new(Loader))
}

func RootCA() []string {
	if Common != nil {
		return Common.RootCACertPaths
	}
	return []string{}
}

func TLSConfig() (*tls.Config, error) {
	if Common != nil && len(Common.RootCACertPaths) > 0 {
		tlsConfig, err := Common.TLSConfig()
		if err != nil {
			return nil, xerrors.Errorf("unable to load tls: %w", err)
		}
		return tlsConfig, nil
	}
	return new(tls.Config), nil
}

var (
	_ config.SecretResolverFactory = (*SecretResolverFactory)(nil)
)

type SecretResolverFactory struct{}

func (f SecretResolverFactory) NewAWSSecretManagerResolver(configBundle config.ConfigBundle) (config.AWSSecretManagerResolver, error) {
	awsConfig := (*configBundle.AWS.(**AWSConfig))
	return config.NewAWSSecretManagerResolver(awsConfig.Profile, awsConfig.ResolveAssumeRole(), awsConfig.AssumeChain()...)
}

func (SecretResolverFactory) NewLockboxResolver(configBundle config.ConfigBundle) (config.LockboxSecretResolver, error) {
	creds := (*configBundle.Cloud.(**CloudConfig)).CloudCreds
	yandexCloudEndpoint := (*configBundle.Cloud.(**CloudConfig)).CloudAPIEndpoint
	return config.NewLockboxResolver(creds, yandexCloudEndpoint)
}

func (SecretResolverFactory) NewAWSResolver(configBundle config.ConfigBundle) (config.AWSSecretResolver, error) {
	awsConfig := (*configBundle.AWS.(**AWSConfig))
	keyID := awsConfig.KeyID
	if keyID == "" {
		return nil, xerrors.New("key_id not given")
	}
	return config.NewAWSResolver(keyID, awsConfig.Profile, awsConfig.ResolveAssumeRole(), awsConfig.AssumeChain()...)
}

func (SecretResolverFactory) NewYavResolver(configBundle config.ConfigBundle) (config.YavSecretResolver, error) {
	yavToken := string((*configBundle.Cloud.(**CloudConfig)).YavToken)
	if yavToken == "" {
		return nil, xerrors.New("yav_token not given")
	}
	return config.NewYavResolver(yavToken)
}

func (SecretResolverFactory) NewKMSResolver(configBundle config.ConfigBundle) (config.KMSSecretResolver, error) {
	creds := (*configBundle.Cloud.(**CloudConfig)).CloudCreds
	yandexCloudEndpoint := (*configBundle.Cloud.(**CloudConfig)).CloudAPIEndpoint
	kmsKeyID := (*configBundle.ExternalCloud.(**ExternalCloudConfig)).KMSKeyID
	kmsDiscoveryEndpoint := (*configBundle.ExternalCloud.(**ExternalCloudConfig)).KMSDiscoveryEndpoint
	return config.NewKMSResolver(creds, yandexCloudEndpoint, kmsKeyID, kmsDiscoveryEndpoint)
}

func (SecretResolverFactory) NewMetadataResolver(configBundle config.ConfigBundle) (config.MetadataSecretResolver, error) {
	return config.NewMetadataResolver()
}
