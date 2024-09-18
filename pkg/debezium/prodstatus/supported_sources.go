package prodstatus

import (
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/providers/mysql"
	"github.com/doublecloud/transfer/pkg/providers/postgres"
	"github.com/doublecloud/transfer/pkg/providers/ydb"
)

var supportedSources = map[string]bool{
	postgres.ProviderType.Name(): true,
	mysql.ProviderType.Name():    true,
	ydb.ProviderType.Name():      true,
}

func IsSupportedSource(src string, _ abstract.TransferType) bool {
	return supportedSources[src]
}
