package sample

import (
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
)

type StreamingData interface {
	TableName() abstract.TableID
	ToChangeItem(offset int64) abstract.ChangeItem
}
