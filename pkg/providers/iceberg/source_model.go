package iceberg

import (
	"github.com/apache/iceberg-go"
	"github.com/doublecloud/transfer/pkg/abstract"
)

type IcebergSource struct {
	Properties  iceberg.Properties
	CatalogType string
	CatalogURI  string
	Schema      string
}

func (i IcebergSource) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (i IcebergSource) Validate() error {
	return nil
}

func (i IcebergSource) WithDefaults() {
}

func (i IcebergSource) IsSource() {
}
