package changeitem

import (
	"time"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/schema"
)

const (
	RawMessageTopic     = "topic"
	RawMessagePartition = "partition"
	RawMessageSeqNo     = "seq_no"
	RawMessageWriteTime = "write_time"
	RawMessageData      = "data"

	OriginalTypeMirrorBinary = "mirror:binary"
)

var (
	RawDataSchema = NewTableSchema([]ColSchema{
		{ColumnName: RawMessageTopic, DataType: string(schema.TypeString), PrimaryKey: true, Required: true},
		{ColumnName: RawMessagePartition, DataType: string(schema.TypeUint32), PrimaryKey: true, Required: true},
		{ColumnName: RawMessageSeqNo, DataType: string(schema.TypeUint64), PrimaryKey: true, Required: true},
		{ColumnName: RawMessageWriteTime, DataType: string(schema.TypeDatetime), PrimaryKey: true, Required: true},
		{ColumnName: RawMessageData, DataType: string(schema.TypeString), OriginalType: OriginalTypeMirrorBinary},
	})
	RawDataColumns = []string{RawMessageTopic, RawMessagePartition, RawMessageSeqNo, RawMessageWriteTime, RawMessageData}
	RawDataColsIDX = ColIDX(RawDataSchema.Columns())
)

func MakeRawMessage(table string, commitTime time.Time, topic string, shard int, offset int64, data []byte) ChangeItem {
	return ChangeItem{
		ID:          0,
		Kind:        InsertKind,
		Counter:     0,
		CommitTime:  uint64(commitTime.UnixNano()),
		LSN:         uint64(offset),
		TableSchema: RawDataSchema,
		ColumnNames: RawDataColumns,
		Schema:      "",
		OldKeys:     EmptyOldKeys(),
		TxID:        "",
		Query:       "",
		Table:       table,
		PartID:      "",
		ColumnValues: []interface{}{
			topic,
			shard,
			uint64(offset),
			commitTime,
			string(data),
		},
		Size: RawEventSize(uint64(len(data))),
	}
}

func GetRawMessageData(r ChangeItem) ([]byte, error) {
	switch v := r.ColumnValues[RawDataColsIDX[RawMessageData]].(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return nil, xerrors.Errorf("unexpected data type: %T, expected string or []byte", v)
	}
}

func ColIDX(schema []ColSchema) map[string]int {
	res := map[string]int{}
	for i := range schema {
		res[schema[i].ColumnName] = i
	}
	return res
}
