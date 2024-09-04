package logbroker

import (
	"fmt"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	debeziumparameters "github.com/doublecloud/transfer/transfer_manager/go/pkg/debezium/parameters"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/middlewares/async/bufferer"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/ydb"
)

type LbDestination struct {
	Instance string
	Database string

	Token             string
	Shard             string
	TLS               TLSMode
	TransformerConfig map[string]string
	Cleanup           server.CleanupType
	MaxChunkSize      uint // Deprecated, can be deleted, but I'm scared by the GOB
	WriteTimeoutSec   int
	Credentials       ydb.TokenCredentials
	Port              int

	Topic       string // full-name version
	TopicPrefix string

	AddSystemTables bool // private options - to not skip consumer_keeper & other system tables
	SaveTxOrder     bool

	// for now, 'FormatSettings' is private option - it's WithDefaults(): SerializationFormatAuto - 'Mirror' for queues, 'Debezium' for the rest
	FormatSettings server.SerializationFormat

	RootCAFiles []string
}

var _ server.Destination = (*LbDestination)(nil)

type TLSMode string

const (
	DefaultTLS  TLSMode = "Default"
	EnabledTLS  TLSMode = "Enabled"
	DisabledTLS TLSMode = "Disabled"
)

func (d *LbDestination) IsEmpty() bool {
	// Case for function 'getEndpointsCreateFormDefaultsDynamic'
	// In this case 'KafkaDestination' model is initialized by default values, and we can set defaults for one-of
	return d.Topic == "" && d.TopicPrefix == ""
}

func (d *LbDestination) WithDefaults() {
	if d.Cleanup == "" {
		d.Cleanup = server.DisabledCleanup
	}
	if d.TLS == "" {
		d.TLS = DefaultTLS
	}
	if d.WriteTimeoutSec == 0 {
		d.WriteTimeoutSec = 30
	}
	if d.FormatSettings.Name == "" {
		d.FormatSettings.Name = server.SerializationFormatAuto
	}
	if d.FormatSettings.Settings == nil {
		d.FormatSettings.Settings = make(map[string]string)
	}
	if d.FormatSettings.BatchingSettings == nil {
		d.FormatSettings.BatchingSettings = &server.Batching{
			Enabled:        true,
			Interval:       0,
			MaxChangeItems: 1000,
			MaxMessageSize: 0,
		}
	}
}

func (d *LbDestination) CleanupMode() server.CleanupType {
	return d.Cleanup
}

func (d *LbDestination) Transformer() map[string]string {
	return d.TransformerConfig
}

func (LbDestination) IsDestination() {}

func (d *LbDestination) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (d *LbDestination) Validate() error {
	if d.TopicPrefix != "" && d.SaveTxOrder {
		return xerrors.Errorf("option 'SaveTxOrder'=true is incompatible with 'TopicPrefix'. Use either full topic name or turn off 'SaveTxOrder'.")
	}
	return nil
}

func (d *LbDestination) Compatible(src server.Source, transferType abstract.TransferType) error {
	return sourceCompatible(src, transferType, d.FormatSettings.Name)
}

func (tm TLSMode) IsValid() error {
	switch tm {
	case DefaultTLS, EnabledTLS, DisabledTLS:
		return nil
	}
	return fmt.Errorf("invalid TLS mode: %v", tm)
}

func (d *LbDestination) IsTransitional() {}

func (d *LbDestination) TransitionalWith(left server.TransitionalEndpoint) bool {
	if src, ok := left.(*LbSource); ok {
		return d.Instance == src.Instance && d.Topic == src.Topic
	}
	return false
}

func (d *LbDestination) Serializer() (server.SerializationFormat, bool) {
	formatSettings := d.FormatSettings
	formatSettings.Settings = debeziumparameters.GetDefaultParameters(formatSettings.Settings)
	return formatSettings, d.SaveTxOrder
}

func (d *LbDestination) BuffererConfig() bufferer.BuffererConfig {
	return bufferer.BuffererConfig{
		TriggingCount:    d.FormatSettings.BatchingSettings.MaxChangeItems,
		TriggingSize:     uint64(d.FormatSettings.BatchingSettings.MaxMessageSize),
		TriggingInterval: d.FormatSettings.BatchingSettings.Interval,
	}
}
