package events

import (
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/base"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/yt/tablemeta"
)

type tableEvent struct {
	path *tablemeta.YtTableMeta
}

type TableEvent interface {
	base.Event
	Table() *tablemeta.YtTableMeta
}

func (t *tableEvent) Table() *tablemeta.YtTableMeta {
	return t.path
}

func newTableEvent(path *tablemeta.YtTableMeta) TableEvent {
	return &tableEvent{path}
}
