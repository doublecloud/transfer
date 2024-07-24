package yds

import (
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
	debeziumparameters "github.com/doublecloud/tross/transfer_manager/go/pkg/debezium/parameters"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/middlewares/async/bufferer"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/logbroker"
)

type YDSDestination struct {
	LbDstConfig      logbroker.LbDestination
	SubNetworkID     string
	SecurityGroupIDs []string

	RootCAFiles []string
	TLSEnalbed  bool

	// Auth properties
	ServiceAccountID string
	SAKeyContent     string
	TokenServiceURL  string
	Token            server.SecretString
	UserdataAuth     bool
}

var _ server.Destination = (*YDSDestination)(nil)

// EndpointParams

func (d *YDSDestination) MDBClusterID() string {
	result := d.LbDstConfig.Database + "/" + d.LbDstConfig.Topic
	if result == "/" {
		return ""
	}
	return result
}

func (d *YDSDestination) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (d *YDSDestination) Validate() error {
	if d.LbDstConfig.TopicPrefix != "" && d.LbDstConfig.SaveTxOrder {
		return xerrors.Errorf("option 'SaveTxOrder'=true is incompatible with 'TopicPrefix'. Use either full topic name or turn off 'SaveTxOrder'.")
	}
	return nil
}

func (d *YDSDestination) Compatible(src server.Source, transferType abstract.TransferType) error {
	return sourceCompatible(src, transferType, d.LbDstConfig.FormatSettings.Name)
}

func (d *YDSDestination) WithDefaults() {
	d.LbDstConfig.WithDefaults()
	d.LbDstConfig.Port = 2135
}

// Destination

func (d *YDSDestination) IsDestination() {}

func (d *YDSDestination) Transformer() map[string]string {
	return d.LbDstConfig.TransformerConfig
}

func (d *YDSDestination) CleanupMode() server.CleanupType {
	return d.LbDstConfig.Cleanup
}

// other

func (d *YDSDestination) Serializer() (server.SerializationFormat, bool) {
	formatSettings := d.LbDstConfig.FormatSettings
	formatSettings.Settings = debeziumparameters.GetDefaultParameters(formatSettings.Settings)
	return formatSettings, d.LbDstConfig.SaveTxOrder
}

func (d *YDSDestination) BuffererConfig() bufferer.BuffererConfig {
	return bufferer.BuffererConfig{
		TriggingCount:    d.LbDstConfig.FormatSettings.BatchingSettings.MaxChangeItems,
		TriggingSize:     uint64(d.LbDstConfig.FormatSettings.BatchingSettings.MaxMessageSize),
		TriggingInterval: d.LbDstConfig.FormatSettings.BatchingSettings.Interval,
	}
}
