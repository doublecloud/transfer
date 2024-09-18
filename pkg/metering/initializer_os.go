//go:build !arcadia
// +build !arcadia

package metering

import (
	"github.com/doublecloud/transfer/internal/logger"
	server "github.com/doublecloud/transfer/pkg/abstract/model"
)

func Agent() MeteringAgent {
	commonAgentMu.Lock()
	defer commonAgentMu.Unlock()
	return NewStubAgent(logger.Log)
}

func InitializeWithTags(transfer *server.Transfer, task *server.TransferOperation, runtimeTags map[string]interface{}) {
	return
}

func WithAgent(agent MeteringAgent) MeteringAgent {
	commonAgentMu.Lock()
	defer commonAgentMu.Unlock()
	commonAgent = agent
	return commonAgent
}

func Initialize(transfer *server.Transfer, task *server.TransferOperation) {
	InitializeWithTags(transfer, task, map[string]interface{}{})
}
