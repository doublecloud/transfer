package filter

import (
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
)

type ListableFilter interface {
	ListTables() ([]abstract.TableID, error)
}

type FilterableFilter interface {
	ListFilters() ([]abstract.TableDescription, error)
}
