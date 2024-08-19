package tablequery

import (
	"context"

	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
)

// StorageTableQueryable is storage with table query loading
type StorageTableQueryable interface {
	abstract.SampleableStorage

	LoadQueryTable(ctx context.Context, table TableQuery, pusher abstract.Pusher) error
}
