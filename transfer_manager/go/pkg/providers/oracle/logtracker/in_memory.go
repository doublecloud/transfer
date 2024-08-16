package logtracker

import (
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/oracle/common"
)

type InMemoryLogTracker struct {
	transferID string
	position   *common.LogPosition
}

func NewInMemoryLogTracker(transferID string) (*InMemoryLogTracker, error) {
	tracker := &InMemoryLogTracker{
		transferID: transferID,
		position:   nil,
	}
	return tracker, nil
}

func (tracker *InMemoryLogTracker) TransferID() string {
	return tracker.transferID
}

func (tracker *InMemoryLogTracker) Init() error {
	return nil
}

func (tracker *InMemoryLogTracker) ClearPosition() error {
	tracker.position = nil
	return nil
}

func (tracker *InMemoryLogTracker) ReadPosition() (*common.LogPosition, error) {
	return tracker.position, nil
}

func (tracker *InMemoryLogTracker) WritePosition(position *common.LogPosition) error {
	tracker.position = position
	return nil
}
