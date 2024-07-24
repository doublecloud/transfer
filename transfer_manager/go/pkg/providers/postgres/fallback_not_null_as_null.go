package postgres

import (
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/typesystem"
)

func FallbackNotNullAsNull(ci *abstract.ChangeItem) (*abstract.ChangeItem, error) {
	if !ci.IsRowEvent() {
		return ci, typesystem.FallbackDoesNotApplyErr
	}

	fallbackApplied := false
	for i := 0; i < len(ci.TableSchema.Columns()); i++ {
		if ci.TableSchema.Columns()[i].Required {
			ci.TableSchema.Columns()[i].Required = false
			fallbackApplied = true
		}
	}
	if !fallbackApplied {
		return ci, typesystem.FallbackDoesNotApplyErr
	}
	return ci, nil
}

func init() {
	typesystem.AddFallbackSourceFactory(func() typesystem.Fallback {
		return typesystem.Fallback{
			To:           2,
			ProviderType: ProviderType,
			Function:     FallbackNotNullAsNull,
		}
	})
}
