package datadog

import (
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
)

type DatadogDestination struct {
	ClientAPIKey server.SecretString
	DatadogHost  string

	// mapping to columns
	SourceColumn    string
	TagColumns      []string
	HostColumn      string
	ServiceColumn   string
	MessageTemplate string
	ChunkSize       int
}

var _ server.Destination = (*DatadogDestination)(nil)

func (d *DatadogDestination) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (d *DatadogDestination) Validate() error {
	return nil
}

func (d *DatadogDestination) WithDefaults() {
	if d.ChunkSize == 0 {
		d.ChunkSize = 500
	}
}

func (d *DatadogDestination) CleanupMode() server.CleanupType {
	return server.DisabledCleanup
}

func (d *DatadogDestination) Compatible(src server.Source, transferType abstract.TransferType) error {
	if _, ok := src.(server.AppendOnlySource); ok {
		return nil
	}
	return xerrors.Errorf("%T is not compatible with Datadog, only append only source allowed", src)
}

func (d *DatadogDestination) IsDestination() {
}
