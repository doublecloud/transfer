package postgres

import (
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/typesystem"
	"go.ytsaurus.tech/yt/go/schema"
)

func init() {
	typesystem.AddFallbackSourceFactory(func() typesystem.Fallback {
		return typesystem.Fallback{
			To:           1,
			ProviderType: ProviderType,
			Function: func(ci *abstract.ChangeItem) (*abstract.ChangeItem, error) {
				if !ci.IsRowEvent() {
					switch ci.Kind {
					case abstract.InitTableLoad, abstract.DoneTableLoad:
						// perform fallback
					default:
						return ci, typesystem.FallbackDoesNotApplyErr
					}
				}

				fallbackApplied := false
				for i := 0; i < len(ci.TableSchema.Columns()); i++ {
					switch ci.TableSchema.Columns()[i].DataType {
					case schema.TypeDate.String(), schema.TypeDatetime.String(), schema.TypeTimestamp.String():
						fallbackApplied = true
						ci.TableSchema.Columns()[i].DataType = schema.TypeString.String()
					default:
						// do nothing
					}
				}
				if !fallbackApplied {
					return ci, typesystem.FallbackDoesNotApplyErr
				}
				return ci, nil
			},
		}
	})
	// for Greenplum, no fallback is necessary because it always set the "EmitTimeTypes" property for PG storage it constructed
}
