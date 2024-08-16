package queue

import (
	"encoding/json"
	"testing"

	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
)

func TestJSONBatcher(t *testing.T) {
	commonTest(t, func(batchingSettings server.Batching) Serializer {
		result, _ := NewJSONSerializer(batchingSettings, false, logger.Log)
		return result
	}, func(in abstract.ChangeItem) int {
		after := make(map[string]interface{})
		for i := range in.ColumnNames {
			after[in.ColumnNames[i]] = in.ColumnValues[i]
		}
		buf, _ := json.Marshal(after)
		return len(buf)
	}, func(elementsSize, elementsNum int) int64 {
		newLinesNum := int64(elementsNum - 1)
		return newLinesNum + int64(elementsSize*elementsNum)
	})
}
