package permissions

type Permission string

const (
	TransferGet            = Permission("data-transfer.transfers.get")
	TransferCreate         = Permission("data-transfer.transfers.create")
	TransferUpdate         = Permission("data-transfer.transfers.update")
	TransferDelete         = Permission("data-transfer.transfers.delete")
	TransferDeactivate     = Permission("data-transfer.transfers.deactivate")
	TransferUse            = Permission("data-transfer.transfers.use")
	TransferCreateExternal = Permission("data-transfer.transfers.createExternal")
	TransferUpdateVersion  = Permission("data-transfer.transfers.updateVersion")
	TransferFreezeVersion  = Permission("data-transfer.transfers.freezeVersion")

	EndpointGet    = Permission("data-transfer.endpoints.get")
	EndpointCreate = Permission("data-transfer.endpoints.create")
	EndpointUpdate = Permission("data-transfer.endpoints.update")
	EndpointDelete = Permission("data-transfer.endpoints.delete")

	QuotaGet    = Permission("data-transfer.quotas.get")
	QuotaUpdate = Permission("data-transfer.quotas.update")

	DataPlaneHeadVersionGet    = Permission("data-transfer.backends.getDataplaneHeadVersion")
	DataPlaneHeadVersionUpdate = Permission("data-transfer.backends.updateDataplaneHeadVersion")
	DataPlaneVersionCreate     = Permission("data-transfer.backends.createDataplaneVersion")
	DataPlaneVersionGet        = Permission("data-transfer.backends.getDataplaneVersion")

	VPCUseSubnets        = Permission("vpc.subnets.use")
	VPCUseSecurityGroups = Permission("vpc.securityGroups.use")

	IAMServiceAccountsUse = Permission("iam.serviceAccounts.use")

	ManagedClickhouseGet    = Permission("managed-clickhouse.clusters.get")
	ManagedElasticsearchGet = Permission("managed-elasticsearch.clusters.get")
	ManagedGreenplumGet     = Permission("managed-greenplum.clusters.get")
	ManagedKafkaGet         = Permission("managed-kafka.clusters.get")
	ManagedMongoGet         = Permission("managed-mongodb.clusters.get")
	ManagedMysqlGet         = Permission("managed-mysql.clusters.get")
	ManagedOpensearchGet    = Permission("managed-opensearch.clusters.get")
	ManagedPgGet            = Permission("managed-postgresql.clusters.get")

	YDBDatabasesGet = Permission("ydb.databases.get")

	LogbrokerEndpointCreate = Permission("data-transfer.endpoints.allow.logbroker")
	LogfellerEndpointCreate = Permission("data-transfer.endpoints.allow.logfeller")
	YtEndpointCreate        = Permission("data-transfer.endpoints.allow.yt")
	AirbyteEndpointCreate   = Permission("data-transfer.endpoints.allow.airbyte")

	YcToLogbroker   = Permission("data-transfer.endpoints.allow.yc-to-logbroker")
	YcFromLogbroker = Permission("data-transfer.endpoints.allow.yc-from-logbroker")
	MetrikaToKafka  = Permission("data-transfer.endpoints.allow.metrika-to-kafka")
)
