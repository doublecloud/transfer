package dataplane

import (
	_ "github.com/doublecloud/transfer/pkg/providers/airbyte"
	_ "github.com/doublecloud/transfer/pkg/providers/clickhouse"
	_ "github.com/doublecloud/transfer/pkg/providers/coralogix"
	_ "github.com/doublecloud/transfer/pkg/providers/datadog"
	_ "github.com/doublecloud/transfer/pkg/providers/delta"
	_ "github.com/doublecloud/transfer/pkg/providers/elastic"
	_ "github.com/doublecloud/transfer/pkg/providers/eventhub"
	_ "github.com/doublecloud/transfer/pkg/providers/greenplum"
	_ "github.com/doublecloud/transfer/pkg/providers/kafka"
	_ "github.com/doublecloud/transfer/pkg/providers/mongo"
	_ "github.com/doublecloud/transfer/pkg/providers/mysql"
	_ "github.com/doublecloud/transfer/pkg/providers/opensearch"
	_ "github.com/doublecloud/transfer/pkg/providers/postgres"
	_ "github.com/doublecloud/transfer/pkg/providers/s3/provider"
	_ "github.com/doublecloud/transfer/pkg/providers/stdout"
	_ "github.com/doublecloud/transfer/pkg/providers/ydb"
	_ "github.com/doublecloud/transfer/pkg/providers/yt/init"
)
