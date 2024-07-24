package schema

import (
	"github.com/doublecloud/tross/transfer_manager/go/pkg/base"
)

type DDLBatch struct {
	DDLs []TableDDL
	iter int
}

func (b *DDLBatch) Next() bool {
	b.iter++
	return len(b.DDLs) < b.iter
}

func (b *DDLBatch) Event() (base.Event, error) {
	return b.DDLs[b.iter], nil
}

func NewDDLBatch(ddls []TableDDL) *DDLBatch {
	return &DDLBatch{
		DDLs: ddls,
		iter: -1,
	}
}
