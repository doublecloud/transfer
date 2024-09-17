package mysql

import (
	"strings"
	"time"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/providers/mysql/unmarshaller/replication"
)

func CastRowsToDT(event *RowsEvent, location *time.Location) error {
	for columnIndex := range event.Table.Columns {
		originalType := event.GetColumnRawType(columnIndex)
		switch {
		case strings.HasPrefix(originalType, "enum"):
			castEnumInPlace(event, columnIndex)
		case strings.HasPrefix(originalType, "set"):
			castSetInPlace(event, columnIndex)
		default:
			if err := castCommonInPlace(event, columnIndex, originalType, location); err != nil {
				return xerrors.Errorf("failed to cast column '%s': %w", event.Table.Columns[columnIndex].Name, err)
			}
		}
	}
	return nil
}

func castEnumInPlace(event *RowsEvent, columnIndex int) {
	for _, row := range event.Data.Rows {
		enumIndex, ok := row[columnIndex].(int64)
		if !ok {
			continue
		}
		if enumIndex == 0 {
			row[columnIndex] = ""
		} else {
			row[columnIndex] = event.GetColumnEnumValue(columnIndex, enumIndex-1)
		}
	}
}

func castSetInPlace(event *RowsEvent, columnIndex int) {
	for _, row := range event.Data.Rows {
		flags, ok := row[columnIndex].(int64)
		if !ok {
			continue
		}
		if flags == 0 {
			row[columnIndex] = ""
		} else {
			var builder strings.Builder
			for i := 0; i < 64; i++ {
				flag := int64(1) << i
				if flags&flag == 0 {
					continue
				}
				if builder.Len() > 0 {
					builder.WriteString(",")
				}
				builder.WriteString(event.GetColumnSetValue(columnIndex, i))
			}
			row[columnIndex] = builder.String()
		}
	}
}

func castCommonInPlace(event *RowsEvent, columnIndex int, originalType string, location *time.Location) error {
	unmarshalSchema := abstract.NewColSchema(event.GetColumnName(columnIndex), TypeToYt(originalType), false)
	unmarshalSchema.OriginalType = originalType
	for rowIndex := range event.Data.Rows {
		value := event.Data.Rows[rowIndex][columnIndex]
		result, err := replication.UnmarshalHetero(value, &unmarshalSchema, location)
		if err != nil {
			return xerrors.Errorf("failed to unmarshal row %d, field %d: %w", rowIndex, columnIndex, err)
		}
		event.Data.Rows[rowIndex][columnIndex] = result
	}
	return nil
}
