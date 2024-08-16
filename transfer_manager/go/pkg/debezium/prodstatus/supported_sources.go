package prodstatus

import (
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/mysql"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/postgres"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/ydb"
)

var supportedSources = map[string]bool{
	postgres.ProviderType.Name(): true,
	mysql.ProviderType.Name():    true,
	ydb.ProviderType.Name():      true,
}

func IsSupportedSource(src string, _ abstract.TransferType) bool {
	return supportedSources[src]
}
