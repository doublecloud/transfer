package kafka

import (
	"time"

	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"go.ytsaurus.tech/yt/go/schema"
)

const (
	kafkaRawMessageTopic     = "topic"
	kafkaRawMessagePartition = "partition"
	kafkaRawMessageOffset    = "offset"
	kafkaRawMessageWriteTime = "write_time"
	kafkaRawMessageKey       = "key"
	kafkaRawMessageData      = "data"
)

var (
	kafkaRawDataSchema = abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: kafkaRawMessageTopic, DataType: string(schema.TypeString), PrimaryKey: true, Required: true},
		{ColumnName: kafkaRawMessagePartition, DataType: string(schema.TypeUint32), PrimaryKey: true, Required: true},
		{ColumnName: kafkaRawMessageOffset, DataType: string(schema.TypeUint64), PrimaryKey: true, Required: true},
		{ColumnName: kafkaRawMessageWriteTime, DataType: string(schema.TypeDatetime), PrimaryKey: true, Required: true},
		{ColumnName: kafkaRawMessageKey, DataType: string(schema.TypeBytes)},
		{ColumnName: kafkaRawMessageData, DataType: string(schema.TypeBytes)},
	})
	kafkaRawDataColumns = []string{kafkaRawMessageTopic, kafkaRawMessagePartition, kafkaRawMessageOffset, kafkaRawMessageWriteTime, kafkaRawMessageKey, kafkaRawMessageData}
	kafkaRawDataColsIDX = abstract.ColIDX(kafkaRawDataSchema.Columns())
)

func IsKafkaRawMessage(items []abstract.ChangeItem) bool {
	if len(items) == 0 {
		return false
	}
	return items[0].TableSchema == kafkaRawDataSchema
}

func MakeKafkaRawMessage(table string, commitTime time.Time, topic string, shard int, offset int64, key, data []byte) abstract.ChangeItem {
	return abstract.ChangeItem{
		ID:          0,
		Kind:        abstract.InsertKind,
		Counter:     0,
		CommitTime:  uint64(commitTime.UnixNano()),
		LSN:         uint64(offset),
		TableSchema: kafkaRawDataSchema,
		ColumnNames: kafkaRawDataColumns,
		Schema:      "",
		OldKeys:     abstract.EmptyOldKeys(),
		TxID:        "",
		Query:       "",
		Table:       table,
		PartID:      "",
		ColumnValues: []interface{}{
			topic,
			shard,
			uint64(offset),
			commitTime,
			key,
			data,
		},
		Size: abstract.RawEventSize(uint64(len(data))),
	}
}

func GetKafkaRawMessageKey(r *abstract.ChangeItem) []byte {
	return r.ColumnValues[kafkaRawDataColsIDX[kafkaRawMessageKey]].([]byte)
}

func GetKafkaRawMessageData(r *abstract.ChangeItem) []byte {
	return r.ColumnValues[kafkaRawDataColsIDX[kafkaRawMessageData]].([]byte)
}
