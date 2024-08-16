package logbroker

import (
	"encoding/binary"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue"
)

func BatchStatistics(batches []persqueue.MessageBatch) (size int64, count int64) {
	for _, batch := range batches {
		for _, m := range batch.Messages {
			count += 1
			size += int64(binary.Size(m.Data))
		}
	}
	return size, count
}
