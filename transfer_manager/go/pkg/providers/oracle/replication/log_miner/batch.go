package logminer

import (
	"time"

	"github.com/doublecloud/transfer/transfer_manager/go/pkg/base"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/oracle/common"
)

const (
	maxBatchSize = 50_000
	maxBatchTime = time.Second * 5
)

type logMinerBatch struct {
	Rows             []base.Event
	CreateTime       time.Time
	ProgressPosition *common.LogPosition
}

func newLogMinerBatch() *logMinerBatch {
	return &logMinerBatch{
		Rows:             []base.Event{},
		CreateTime:       time.Now(),
		ProgressPosition: nil,
	}
}

func (batch *logMinerBatch) Empty() bool {
	return len(batch.Rows) == 0
}

func (batch *logMinerBatch) SetProgressPosition(position *common.LogPosition) {
	if position == nil {
		return
	}
	batch.ProgressPosition = position
}

func (batch *logMinerBatch) HasProgressPosition() bool {
	return batch.ProgressPosition != nil
}

func (batch *logMinerBatch) Ready() bool {
	return len(batch.Rows) >= maxBatchSize || time.Since(batch.CreateTime) >= maxBatchTime
}

func (batch *logMinerBatch) Add(event base.Event) {
	batch.Rows = append(batch.Rows, event)
}
