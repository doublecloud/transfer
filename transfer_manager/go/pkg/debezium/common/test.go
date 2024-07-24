package common

import "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"

type ChangeItemCanon struct {
	ChangeItem     *abstract.ChangeItem
	DebeziumEvents []KeyValue
}
