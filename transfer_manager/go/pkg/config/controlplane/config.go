package cpconfig

import (
	"crypto/tls"
	_ "embed"
	"io"

	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/config"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/xtls"
)

var (
	// Not nil for controlplane
	Common *CommonConfig

	// Not nil when either Internal != nil or Public != nil
	Cloud *CloudConfig

	// Exactly one of these is not nil
	InternalCloud *InternalCloudConfig
	ExternalCloud *ExternalCloudConfig
	AWS           *AWSConfig
)

var (
	//go:embed installations/external_preprod.yaml
	externalPreprodYaml []byte

	//go:embed installations/external_prod.yaml
	externalProdYaml []byte

	//go:embed installations/external_kz.yaml
	externalKzYaml []byte

	//go:embed installations/internal_prod.yaml
	internalProdYaml []byte

	//go:embed installations/internal_testing.yaml
	internalTestingYaml []byte

	//go:embed installations/external_preprod.k8s.yaml
	externalPreprodK8SYaml []byte

	//go:embed parts/common_local.yaml
	commonLocaParts []byte

	//go:embed parts/dc_preprod_local.yaml
	dcPreprodLocalParts []byte

	//go:embed parts/dc_prod_local.yaml
	dcProdLocalParts []byte

	EmbeddedConfigs = map[string][]byte{
		"parts/dc_preprod_local.yaml": dcPreprodLocalParts,
		"parts/dc_prod_local.yaml":    dcProdLocalParts,
		"external_preprod.yaml":       externalPreprodYaml,
		"external_preprod.k8s.yaml":   externalPreprodK8SYaml,
		"external_prod.yaml":          externalProdYaml,
		"external_kz.yaml":            externalKzYaml,
		"internal_prod.yaml":          internalProdYaml,
		"internal_testing.yaml":       internalTestingYaml,
		"parts/common_local.yaml":     commonLocaParts,
	}
)

type CommonConfig struct {
	DBDiscovery                 config.DBDiscovery      `mapstructure:"db_discovery"`
	DBUser                      string                  `mapstructure:"db_user"`
	DBPassword                  config.Secret           `mapstructure:"db_password"`
	DBName                      string                  `mapstructure:"db_name"`
	DBSchema                    string                  `mapstructure:"db_schema"`
	GRPCPort                    int                     `mapstructure:"grpc_port"`
	PubGRPCPort                 int                     `mapstructure:"public_grpc_port"`
	RESTPort                    int                     `mapstructure:"rest_port"`
	ExportedMetricsHTTPPort     int                     `mapstructure:"exported_metrics_http_port"`
	Log                         config.Log              `mapstructure:"log"`
	Profiler                    config.Profiler         `mapstructure:"profiler"`
	PushMetrics                 bool                    `mapstructure:"push_metrics"`
	SkipAuth                    bool                    `mapstructure:"skip_auth"`
	SyncTasks                   bool                    `mapstructure:"sync_tasks,local_only"`
	LocalReplication            bool                    `mapstructure:"local_replication,local_only"`
	DisableDataplane            bool                    `mapstructure:"disable_dataplane"`
	DataplaneConfigVariableName string                  `mapstructure:"data_plane_config_variable_name"`
	DataplaneConfigOptionValue  string                  `mapstructure:"data_plane_config_variable_value"`
	DataplaneVersion            config.DataplaneVersion `mapstructure:"dataplane_version"`
	DataplaneK8SImage           config.Secret           `mapstructure:"dataplane_image_k8s_image"`
	Environment                 config.Environment      `mapstructure:"environment"`
	HealthProviders             []config.HealthProvider `mapstructure:"health_providers"`
	MetricsProvider             config.MetricsProvider  `mapstructure:"metrics_provider"`
	Search                      config.Search           `mapstructure:"search"`
	Events                      config.Events           `mapstructure:"events"`
}

type CloudConfig struct {
	CloudCreds               config.CloudCreds `mapstructure:"cloud_creds"`
	ConsoleHostname          string            `mapstructure:"console_hostname"`
	YdbLogsInstance          string            `mapstructure:"ydb_logs_instance"`
	YdbLogsDatabase          string            `mapstructure:"ydb_logs_database"`
	RootCACertPaths          []string          `mapstructure:"root_cert_paths"`
	YDBDataplaneLogDir       string            `mapstructure:"ydb_dataplane_log_dir"`
	MDBAPIURLBase            string            `mapstructure:"mdb_api_url_base"`
	IAMAPIURLBase            string            `mapstructure:"iam_api_url_base"`
	CloudAPIEndpoint         string            `mapstructure:"cloud_api_endpoint"`
	IAMAccessServiceEndpoint string            `mapstructure:"iam_access_service_endpoint"`
	LogSourceID              string            `mapstructure:"log_source_id,local_only"`
	S3Endpoint               string            `mapstructure:"s3_endpoint"`
	S3Region                 string            `mapstructure:"s3_region"`
}

type ExternalCloudConfig struct {
	SolomonURL                 string                  `mapstructure:"solomon_url"`
	ServiceAccountID           string                  `mapstructure:"service_account_id"`
	ServiceCloudID             string                  `mapstructure:"service_cloud_id"`
	ServiceFolderID            string                  `mapstructure:"service_folder_id"`
	KMSKeyID                   string                  `mapstructure:"kms_key_id"`
	KMSDiscoveryEndpoint       string                  `mapstructure:"kms_discovery_endpoint"`
	NetID                      string                  `mapstructure:"net_id"`
	TransferDashboardID        string                  `mapstructure:"transfer_dashboard_id"`
	YDSServerlessEndpoint      string                  `mapstructure:"yds_serverless_endpoint"`
	IsProd                     bool                    `mapstructure:"is_prod"`
	YavToken                   config.Secret           `mapstructure:"yav_token,local_only"`
	ServerlessRuntimeConfig    config.Secret           `mapstructure:"serverless_runtime_kubeconfig"`
	DataPlaneVMUsers           VMUsers                 `mapstructure:"data_plane_vm_users"`
	DataPlaneComputePlatformID string                  `mapstructure:"data_plane_compute_platform_id"`
	NTPServers                 string                  `mapstructure:"ntp_servers"`
	FallbackNTP                string                  `mapstructure:"fallback_ntp"`
	MetrikaAPIConfig           config.MetrikaAPIConfig `mapstructure:"metrika_api_config"`
	FineGrainedSSA             config.FineGrainedSSA   `mapstructure:"fine_grained_ssa"`
	IAMTokenServiceEndpoint    string                  `mapstructure:"iam_token_service_endpoint"`
	InstanceServiceEndpoint    string                  `mapstructure:"instance_service_endpoint"`
}

type InternalCloudConfig struct {
	YdbToken                           config.Secret `mapstructure:"ydb_token"`
	MDBToken                           config.Secret `mapstructure:"mdb_token"`
	YavToken                           config.Secret `mapstructure:"yav_token"`
	YtToken                            config.Secret `mapstructure:"yt_token"`
	LogfellerYtToken                   config.Secret `mapstructure:"logfeller_yt_token"`
	LogfellerNirvactorToken            config.Secret `mapstructure:"logfeller_nirvactor_token"`
	LogfellerLogbrokerToken            config.Secret `mapstructure:"logfeller_logbroker_token"`
	LogfellerLfDescriberLogbrokerToken config.Secret `mapstructure:"logfeller_lf_describer_logbroker_token"`
	LogbrokerToken                     config.Secret `mapstructure:"logbroker_token"`
	SolomonToken                       config.Secret `mapstructure:"solomon_token"`
	TLSServerPrivateCertificate        config.Secret `mapstructure:"tls_server_private_certificate"`
	DataplaneToken                     config.Secret `mapstructure:"dataplane_token"`
	SolomonCluster                     string        `mapstructure:"solomon_cluster"`
	GRPCSSLPort                        int           `mapstructure:"grpc_ssl_port"`
	PubGRPCSSLPort                     int           `mapstructure:"public_grpc_ssl_port"`
	YtProxy                            string        `mapstructure:"yt_proxy"`
	YtOperationPool                    string        `mapstructure:"yt_operation_pool"`
	UDFDir                             string        `mapstructure:"udf_dir"`
	TaskMonitorLockPath                string        `mapstructure:"task_monitor_lock_path"`
	SolomonURL                         string        `mapstructure:"solomon_url"`
	SolomonProject                     string        `mapstructure:"solomon_project"`
	DisableJobBinCache                 bool          `mapstructure:"disable_job_bin_cache,local_only"`
	DisableSchemaRegistry              bool          `mapstructure:"disable_schema_registry,local_only"`
	// NotificationsAPI is the address of the HTTP(s) API of the Notify service. Empty or invalid value redirects notifications to log.
	NotificationsAPI string `mapstructure:"notifications_api"`
}

type AWSConfig struct {
	Ignore                bool              `mapstructure:"ignore,local_only"`
	Profile               string            `mapstructure:"profile,local_only"`
	KeyID                 string            `mapstructure:"key_id"`
	GCP                   GCPConfig         `mapstructure:"gcp"`
	AmiConfig             AWSAmiConfig      `mapstructure:"ami_config"`
	Dataplanes            []AWSDataplane    `mapstructure:"dataplanes"`
	CloudCreds            config.CloudCreds `mapstructure:"cloud_creds"`
	IAM                   IAMApis           `mapstructure:"iam"`
	MDBService            string            `mapstructure:"mdb_service"`
	MDBServicePlaintext   bool              `mapstructure:"mdb_service_plaintext,local_only"`
	MdbVpcService         string            `mapstructure:"mdb_vpc_service"`
	DataplaneSA           DataplaneSA       `mapstructure:"dataplane_sa"`
	DataplaneSpecS3Bucket string            `mapstructure:"dataplane_spec_s3_bucket"`
	AssumeRoleTarget      string            `mapstructure:"assume_role_target"`
	// NotificationsAPI is the address of the HTTP(s) API of the Notify service. Empty or invalid value redirects notifications to log.
	NotificationsAPI string `mapstructure:"notifications_api"`

	// AWS-managed Prometheus Service (i.e. AMP)
	AMPRegion           string `mapstructure:"amp_region"`
	AMPURL              string `mapstructure:"amp_url"`
	AMPAssumeRoleTarget string `mapstructure:"amp_assume_role_target"`

	ClustersCPAccountID string `mapstructure:"clusters_cp_account_id"`
	CPAccountID         string `mapstructure:"cp_account_id"`
	DPAccountID         string `mapstructure:"dp_account_id"`

	Teleport config.Teleport `mapstructure:"teleport"`

	OverrideDPConfig string `mapstructure:"override_dp_config,local_only"`
}

type GCPConfig struct {
	ByoaCommonAccount string `mapstructure:"byoa_common_account"`
	ProjectID         string `mapstructure:"project_id"`
	ImageID           string `mapstructure:"image_id"`
	ServiceAccount    string `mapstructure:"service_account"`

	// Creds is json config for OAUTH2 authorization see here: https://cloud.google.com/iam/docs/workload-identity-federation-with-other-clouds#go
	// 	to generate this creds you must call gcloud-cli:
	// 		gcloud iam workload-identity-pools create-cred-config \
	// 			projects/mdb-dp-preprod/locations/global/workloadIdentityPools/mdb-cp/providers/aws \
	// 			--service-account=main-dt-sa-preprod@mdb-dp-preprod.iam.gserviceaccount.com \
	// 			--service-account-token-lifetime-seconds=3600 \
	// 			--aws \
	// 			--output-file=gcp_creds.json
	// this creds is not secret, it's just config. It's use aws auth to gain temporal secret value in runtime.
	Creds string `mapstructure:"creds"`
}

type DataplaneSA struct {
	ID       string `mapstructure:"id"`
	FolderID string `mapstructure:"folder_id"`
}

type IAMApis struct {
	AccessService   string `mapstructure:"access_service"`
	TokenService    string `mapstructure:"token_service"`
	PrivateServices string `mapstructure:"private_service"`
}

type AWSAmiConfig struct {
	InstanceProfile string            `mapstructure:"instance_profile"`
	RegionToAmi     map[string]string `mapstructure:"region_to_ami"`
}

type AWSRegionToAmi struct {
	Region string `mapstructure:"region"`
	Ami    string `mapstructure:"ami"`
}

type AWSDataplane struct {
	Secret config.Secret `mapstructure:"secret"`
	Region string        `mapstructure:"region"`
	Name   string        `mapstructure:"name"`
}

type VMUsers struct {
	SSHKey string   `mapstructure:"ssh_key"`
	Users  []string `mapstructure:"users"`
}

func IsControlPlane() bool {
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

func GetAmiAndIamRole(region string) (ami, iamRole string, err error) {
	if AWS == nil {
		return ami, iamRole, xerrors.Errorf("AWS config is empty, check CP config.")
	}
	for reg, amiID := range AWS.AmiConfig.RegionToAmi {
		if reg == region {
			return amiID, AWS.AmiConfig.InstanceProfile, nil
		}
	}
	return ami, iamRole, xerrors.Errorf("AMI image for region '%s' is not configured, check CP config.", region)
}

func DBSchema() string {
	if Common != nil {
		return Common.DBSchema
	}
	return ""
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

func (c *CloudConfig) TLSConfig() (*tls.Config, error) {
	return xtls.FromPath(c.RootCACertPaths)
}

var (
	_ config.SecretResolverFactory = (*SecretResolverFactory)(nil)
)

type SecretResolverFactory struct{}

func (SecretResolverFactory) NewAWSSecretManagerResolver(configBundle config.ConfigBundle) (config.AWSSecretManagerResolver, error) {
	awsConfig := (*configBundle.AWS.(**AWSConfig))
	return config.NewAWSSecretManagerResolver(awsConfig.Profile, "")
}

func (SecretResolverFactory) NewLockboxResolver(configBundle config.ConfigBundle) (config.LockboxSecretResolver, error) {
	creds := (*configBundle.Cloud.(**CloudConfig)).CloudCreds
	yandexCloudEndpoint := (*configBundle.Cloud.(**CloudConfig)).CloudAPIEndpoint
	return config.NewLockboxResolver(creds, yandexCloudEndpoint)
}

func (f SecretResolverFactory) NewAWSResolver(configBundle config.ConfigBundle) (config.AWSSecretResolver, error) {
	awsConfig := *configBundle.AWS.(**AWSConfig)
	if awsConfig == nil {
		return nil, xerrors.New("aws config is not set")
	}

	keyID := awsConfig.KeyID
	if keyID == "" {
		return nil, xerrors.New("key_id not given")
	}

	return config.NewAWSResolver(keyID, awsConfig.Profile, "")
}

func (SecretResolverFactory) NewYavResolver(configBundle config.ConfigBundle) (config.YavSecretResolver, error) {
	var yavToken string
	if *configBundle.InternalCloud.(**InternalCloudConfig) != nil {
		yavToken = string((*configBundle.InternalCloud.(**InternalCloudConfig)).YavToken)
	} else if *configBundle.ExternalCloud.(**ExternalCloudConfig) != nil {
		yavToken = string((*configBundle.ExternalCloud.(**ExternalCloudConfig)).YavToken)
	}
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
