package kafka

import (
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/typesystem"
	jsonengine "github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers/registry/json/engine"
)

func init() {
	typesystem.AddFallbackSourceFactory(func() typesystem.Fallback {
		return typesystem.Fallback{
			To:           4,
			ProviderType: ProviderType,
			Function:     jsonengine.GenericParserTimestampFallback,
		}
	})
}
