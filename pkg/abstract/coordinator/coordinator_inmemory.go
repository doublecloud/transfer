package coordinator

import (
	"sync"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/pkg/abstract/model"
	"go.ytsaurus.tech/library/go/core/log"
)

type CoordinatorInMemory struct {
	*CoordinatorNoOp

	mu    sync.Mutex
	state map[string]map[string]*TransferStateData

	progress []*model.OperationTablePart
}

func NewStatefulFakeClient() *CoordinatorInMemory {
	return &CoordinatorInMemory{
		CoordinatorNoOp: NewFakeClient(),

		mu:    sync.Mutex{},
		state: map[string]map[string]*TransferStateData{},

		progress: nil,
	}
}

func (f *CoordinatorInMemory) Progress() []*model.OperationTablePart {
	return f.progress
}

func (f *CoordinatorInMemory) UpdateOperationTablesParts(operationID string, tables []*model.OperationTablePart) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.progress = tables
	return nil
}

func (f *CoordinatorInMemory) GetTransferState(id string) (map[string]*TransferStateData, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	logger.Log.Info("get transfer state", log.Any("transfer_id", id), log.Any("state", f.state[id]))
	return f.state[id], nil
}

func (f *CoordinatorInMemory) SetTransferState(transferID string, state map[string]*TransferStateData) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if st, ok := f.state[transferID]; !ok || st == nil {
		f.state[transferID] = state
		logger.Log.Info("set transfer state", log.Any("transfer_id", transferID), log.Any("state", f.state[transferID]))
		return nil
	}
	for stateKey, stateVal := range state {
		f.state[transferID][stateKey] = stateVal
	}
	logger.Log.Info("set transfer state", log.Any("transfer_id", transferID), log.Any("state", f.state[transferID]))
	return nil
}

func (f *CoordinatorInMemory) RemoveTransferState(transferID string, stateKeys []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, stateKey := range stateKeys {
		delete(f.state[transferID], stateKey)
	}
	return nil
}
