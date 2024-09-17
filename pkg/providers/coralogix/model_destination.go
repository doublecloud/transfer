package coralogix

import (
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	server "github.com/doublecloud/transfer/pkg/abstract/model"
)

type CoralogixDestination struct {
	Token  server.SecretString
	Domain string

	MessageTemplate string
	ChunkSize       int
	SubsystemColumn string
	ApplicationName string

	// mapping to columns
	TimestampColumn string
	SourceColumn    string
	CategoryColumn  string
	ClassColumn     string
	MethodColumn    string
	ThreadIDColumn  string
	SeverityColumn  string
	HostColumn      string
	KnownSevereties map[string]Severity
}

func (d *CoralogixDestination) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (d *CoralogixDestination) Validate() error {
	return nil
}

func (d *CoralogixDestination) WithDefaults() {
	if d.ChunkSize == 0 {
		d.ChunkSize = 500
	}
}

func (d *CoralogixDestination) CleanupMode() server.CleanupType {
	return server.DisabledCleanup
}

func (d *CoralogixDestination) Compatible(src server.Source, transferType abstract.TransferType) error {
	if _, ok := src.(server.AppendOnlySource); ok {
		return nil
	}
	return xerrors.Errorf("%T is not compatible with Coralogix, only append only source allowed", src)
}

func (d *CoralogixDestination) IsDestination() {
}
