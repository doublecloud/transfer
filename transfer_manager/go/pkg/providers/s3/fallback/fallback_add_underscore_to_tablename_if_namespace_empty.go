package fallback

import (
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/typesystem"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/s3"
)

func init() {
	typesystem.AddFallbackTargetFactory(func() typesystem.Fallback {
		return typesystem.Fallback{
			To:           8,
			ProviderType: s3.ProviderType,
			Function: func(ci *abstract.ChangeItem) (*abstract.ChangeItem, error) {
				if ci.Schema == "" {
					ci.Table = "_" + ci.Table
				}
				return ci, nil
			},
		}
	})
}
