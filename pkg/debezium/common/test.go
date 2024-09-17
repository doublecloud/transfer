package common

import "github.com/doublecloud/transfer/pkg/abstract"

type ChangeItemCanon struct {
	ChangeItem     *abstract.ChangeItem
	DebeziumEvents []KeyValue
}
