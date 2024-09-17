package snapshot

import (
	"database/sql"
	"reflect"
	"strings"
	"time"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/providers/mysql/unmarshaller/types"
	"github.com/doublecloud/transfer/pkg/util"
)

func NewValueReceiver(k *sql.ColumnType, originalTypeName string, location *time.Location) any {
	switch k.DatabaseTypeName() {
	case "BIGINT", "UNSIGNED BIGINT":
		if strings.HasSuffix(originalTypeName, "unsigned") || strings.HasSuffix(originalTypeName, "zerofill") {
			return new(types.NullUint64)
		}
	case "JSON":
		return new(types.JSON)
	case "DATE", "DATETIME":
		return types.NewTemporal()
	case "TIMESTAMP":
		return types.NewTemporalInLocation(location)
	}
	return reflect.New(k.ScanType()).Interface()
}

func UnmarshalHetero(receivers []any, table []abstract.ColSchema) ([]any, error) {
	return unmarshal(receivers, table, unmarshalHetero)
}

func UnmarshalHomo(receivers []any, table []abstract.ColSchema) ([]any, error) {
	return unmarshal(receivers, table, unmarshalHomo)
}

func unmarshal(receivers []any, table []abstract.ColSchema, unmarshalFieldFunc func(any, *abstract.ColSchema) (any, error)) ([]any, error) {
	result := make([]any, len(receivers))
	errors := util.Errors{}
	for i := range receivers {
		unmarshallingResult, err := unmarshalFieldFunc(receivers[i], &table[i])
		if err != nil {
			errors = append(errors, xerrors.Errorf("column [%d] %q: %w", i, table[i].ColumnName, err))
			continue
		}
		result[i] = unmarshallingResult
	}
	if len(errors) > 0 {
		return nil, xerrors.Errorf("failed to unmarshal: %w", errors)
	}
	return result, nil
}
