//go:build !arcadia
// +build !arcadia

package metering

import (
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
)

func Agent() MeteringAgent {
	commonAgentMu.Lock()
	defer commonAgentMu.Unlock()
	return NewStubAgent(logger.Log)
}

func InitializeWithTags(transfer *server.Transfer, task *server.TransferOperation, runtimeTags map[string]interface{}) {
	return
}

func Initialize(transfer *server.Transfer, task *server.TransferOperation) {
	InitializeWithTags(transfer, task, map[string]interface{}{})
}
