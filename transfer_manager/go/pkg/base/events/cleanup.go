package events

import (
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
)

// TODO: Remove after new cleanup design based on TableLoadEvents
type CleanupEvent abstract.TableID

func (c *CleanupEvent) String() string {
	return "Cleanup"
}
