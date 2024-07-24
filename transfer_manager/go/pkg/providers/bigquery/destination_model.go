package bigquery

import (
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
)

var _ server.Destination = (*BigQueryDestination)(nil)

type BigQueryDestination struct {
	ProjectID     string
	Dataset       string
	Creds         string
	CleanupPolicy server.CleanupType
}

func (b *BigQueryDestination) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (b *BigQueryDestination) Validate() error {
	return nil
}

func (b *BigQueryDestination) WithDefaults() {
	if b.CleanupPolicy == "" {
		b.CleanupPolicy = server.Drop
	}
}

func (b *BigQueryDestination) CleanupMode() server.CleanupType {
	return b.CleanupPolicy
}

func (b *BigQueryDestination) IsDestination() {}
