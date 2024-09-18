package kafka

import (
	"github.com/doublecloud/transfer/pkg/abstract/typesystem"
	jsonengine "github.com/doublecloud/transfer/pkg/parsers/registry/json/engine"
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
