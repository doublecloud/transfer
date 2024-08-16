package dataplane

import (
	_ "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/airbyte"
	_ "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/clickhouse"
	_ "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/coralogix"
	_ "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/datadog"
	_ "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/delta"
	_ "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/elastic"
	_ "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/eventhub"
	_ "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/greenplum"
	_ "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/kafka"
	_ "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/logbroker"
	_ "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/mongo"
	_ "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/mysql"
	_ "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/opensearch"
	_ "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/postgres"
	_ "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/s3/provider"
	_ "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/stdout"
	_ "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/ydb"
	_ "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/yds"
	_ "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/yt/init"
)
