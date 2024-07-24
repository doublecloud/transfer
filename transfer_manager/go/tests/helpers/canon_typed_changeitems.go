package helpers

import (
	"fmt"

	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
)

type row struct {
	Name, DataType, GoType, Value string
}

type CanonTypedChangeItem []row

func ToCanonTypedChangeItem(item abstract.ChangeItem) CanonTypedChangeItem {
	res := make([]row, len(item.ColumnValues))
	for i, col := range item.TableSchema.Columns() {
		res[i] = row{
			Name:     col.ColumnName,
			DataType: col.DataType,
			GoType:   fmt.Sprintf("%T", item.ColumnValues[i]),
			Value:    fmt.Sprint(item.ColumnValues[i]),
		}
	}
	return res
}
