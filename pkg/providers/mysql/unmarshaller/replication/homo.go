package replication

import (
	"time"

	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/util/strict"
	"go.ytsaurus.tech/yt/go/schema"
)

func UnmarshalHomo(value any, colSchema *abstract.ColSchema, _ *time.Location) (any, error) {
	if colSchema.DataType == string(schema.TypeAny) {
		return strict.Expected[string](value, castToAny)
	}
	return value, nil
}
