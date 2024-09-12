package queues

import (
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue"
	"github.com/doublecloud/transfer/library/go/slices"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers"
)

func ConvertBatches(batches []persqueue.MessageBatch) []parsers.MessageBatch {
	return slices.Map(batches, func(t persqueue.MessageBatch) parsers.MessageBatch {
		return parsers.MessageBatch{
			Topic:     t.Topic,
			Partition: t.Partition,
			Messages: slices.Map(t.Messages, func(t persqueue.ReadMessage) parsers.Message {
				return parsers.Message{
					Offset:     t.Offset,
					SeqNo:      t.SeqNo,
					Key:        t.SourceID,
					CreateTime: t.CreateTime,
					WriteTime:  t.WriteTime,
					Value:      t.Data,
					Headers:    t.ExtraFields,
				}
			}),
		}
	})
}
