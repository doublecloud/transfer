package yt

import (
	"time"

	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
)

var _ server.Destination = (*LfStagingDestination)(nil)

type LfStagingDestination struct {
	Cluster           string
	Topic             string
	YtAccount         string
	LogfellerHomePath string
	TmpBasePath       string

	AggregationPeriod time.Duration

	SecondsPerTmpTable int64
	BytesPerTmpTable   int64

	YtToken string

	UsePersistentIntermediateTables bool
	UseNewMetadataFlow              bool
	MergeYtPool                     string
}

func (d *LfStagingDestination) CleanupMode() server.CleanupType {
	return server.DisabledCleanup
}

func (d *LfStagingDestination) Transformer() map[string]string {
	return map[string]string{}
}

func (d *LfStagingDestination) WithDefaults() {

	if d.AggregationPeriod == 0 {
		d.AggregationPeriod = time.Minute * 5
	}

	if d.SecondsPerTmpTable == 0 {
		d.SecondsPerTmpTable = 10
	}

	if d.BytesPerTmpTable == 0 {
		d.BytesPerTmpTable = 20 * 1024 * 1024
	}
}

func (LfStagingDestination) IsDestination() {
}

func (d *LfStagingDestination) GetProviderType() abstract.ProviderType {
	return StagingType
}

func (d *LfStagingDestination) Validate() error {
	return nil
}
