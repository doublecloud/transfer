package coordinator

import (
	"context"
	"sync"
	"time"

	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"go.ytsaurus.tech/library/go/core/log"
)

type FakeClient struct {
	getTransferState func(id string) (map[string]*TransferStateData, error)
}

var _ Coordinator = (*FakeClient)(nil)

func NewFakeClient() *FakeClient {
	return &FakeClient{
		getTransferState: nil,
	}
}

func NewFakeClientWithOpts(
	getTransferState func(id string) (map[string]*TransferStateData, error),
) *FakeClient {
	return &FakeClient{
		getTransferState: getTransferState,
	}
}

func (f *FakeClient) GetTransferState(id string) (map[string]*TransferStateData, error) {
	if f.getTransferState != nil {
		return f.getTransferState(id)
	}
	return nil, nil
}

func (f *FakeClient) SetTransferState(transferID string, state map[string]*TransferStateData) error {
	return nil
}

func (f *FakeClient) RemoveTransferState(transferID string, stateKeys []string) error {
	return nil
}

func (f *FakeClient) SetOperationState(taskID string, state string) error {
	return nil
}

func (f *FakeClient) GetOperationState(taskID string) (string, error) {
	return "", OperationStateNotFoundError
}

func (f *FakeClient) UploadTable(transferID string, tables []abstract.TableDescription) error {
	return nil
}

func (f *FakeClient) FinishOperation(taskID string, shardIndex int, taskErr error) error {
	return nil
}

func (f *FakeClient) OpenStatusMessage(transferID string, category string, content *StatusMessage) error {
	return nil
}

func (f *FakeClient) CloseStatusMessagesForCategory(transferID string, category string) error {
	return nil
}

func (f *FakeClient) CloseStatusMessagesForTransfer(transferID string) error {
	return nil
}

func (f *FakeClient) FailReplication(transferID string, err error) error {
	return nil
}

func (f *FakeClient) TransferHealth(ctx context.Context, transferID string, health *TransferHeartbeat) error {
	return nil
}

func (f *FakeClient) OperationHealth(ctx context.Context, operationID string, workerIndex int, workerTime time.Time) error {
	return nil
}

func (f *FakeClient) UpdateTestResults(id string, request *abstract.TestResult) error {
	return nil
}

func (f *FakeClient) GetTransfers(statuses []server.TransferStatus, transferID string) ([]*server.Transfer, error) {
	return nil, nil
}

func (f *FakeClient) SetStatus(transferID string, status server.TransferStatus) error {
	logger.Log.Infof("fake change status: %v -> %v", transferID, status)
	return nil
}

func (f *FakeClient) GetEndpoint(transferID string, isSource bool) (server.EndpointParams, error) {
	logger.Log.Infof("fake get endpoint: %v", transferID)
	return nil, nil
}

func (f *FakeClient) GetEndpointTransfers(transferID string, isSource bool) ([]*server.Transfer, error) {
	logger.Log.Infof("fake get endpoint transfers: %v", transferID)
	return nil, nil
}

func (f *FakeClient) UpdateEndpoint(transferID string, endpoint server.EndpointParams) error {
	return nil
}

func (f *FakeClient) GetOperationProgress(operationID string) (*server.AggregatedProgress, error) {
	return nil, nil
}

func (f *FakeClient) CreateOperationWorkers(operationID string, workersCount int) error {
	return nil
}

func (f *FakeClient) GetOperationWorkers(operationID string) ([]*server.OperationWorker, error) {
	return nil, nil
}

func (f *FakeClient) GetOperationWorkersCount(operationID string, completed bool) (int, error) {
	return 2, nil
}

func (f *FakeClient) CreateOperationTablesParts(operationID string, tables []*server.OperationTablePart) error {
	return nil
}

func (f *FakeClient) GetOperationTablesParts(operationID string) ([]*server.OperationTablePart, error) {
	return nil, nil
}

func (f *FakeClient) AssignOperationTablePart(operationID string, workerIndex int) (*server.OperationTablePart, error) {
	return nil, nil
}

func (f *FakeClient) ClearAssignedTablesParts(ctx context.Context, operationID string, workerIndex int) (int64, error) {
	return 0, nil
}

func (f *FakeClient) UpdateOperationTablesParts(operationID string, tables []*server.OperationTablePart) error {
	return nil
}

type StatefulFakeClient struct {
	*FakeClient

	mu       sync.Mutex
	progress []*server.OperationTablePart
	state    map[string]map[string]*TransferStateData
}

func NewStatefulFakeClient() *StatefulFakeClient {
	return &StatefulFakeClient{
		FakeClient: NewFakeClient(),

		mu:    sync.Mutex{},
		state: map[string]map[string]*TransferStateData{},
	}
}

func (f *StatefulFakeClient) Progres() []*server.OperationTablePart {
	return f.progress
}

func (f *StatefulFakeClient) UpdateOperationTablesParts(operationID string, tables []*server.OperationTablePart) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.progress = tables
	return nil
}

func (f *StatefulFakeClient) GetTransferState(id string) (map[string]*TransferStateData, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	logger.Log.Info("get transfer state", log.Any("transfer_id", id), log.Any("state", f.state[id]))
	return f.state[id], nil
}

func (f *StatefulFakeClient) SetTransferState(transferID string, state map[string]*TransferStateData) error {
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

func (f *StatefulFakeClient) RemoveTransferState(transferID string, stateKeys []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, stateKey := range stateKeys {
		delete(f.state[transferID], stateKey)
	}
	return nil
}
