package testutil

import (
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
)

// FakeClientWithTransferState is a fake controlplane client which stores sharded object transfer state
type FakeClientWithTransferState struct {
	coordinator.FakeClient
	state map[string]*coordinator.TransferStateData
}

func (c *FakeClientWithTransferState) SetTransferState(transferID string, state map[string]*coordinator.TransferStateData) error {
	c.state = state
	return nil
}

func (c *FakeClientWithTransferState) GetTransferState(transferID string) (map[string]*coordinator.TransferStateData, error) {
	return c.state, nil
}

func NewFakeClientWithTransferState() *FakeClientWithTransferState {
	return &FakeClientWithTransferState{
		FakeClient: coordinator.FakeClient{},
		state:      nil,
	}
}
