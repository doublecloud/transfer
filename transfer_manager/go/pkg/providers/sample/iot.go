package sample

import (
	"time"

	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/util"
	"go.ytsaurus.tech/yt/go/schema"
)

const (
	sampleSourceDeviceColumn = "device"
	sampleSourceNumberColumn = "number"
	sampleSourceTimeColumn   = "time"
)

var (
	_ StreamingData = (*IotData)(nil)

	iotDataSchema = abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: sampleSourceDeviceColumn, DataType: schema.TypeString.String(), Required: true, PrimaryKey: true},
		{ColumnName: sampleSourceNumberColumn, DataType: schema.TypeInt8.String(), Required: true},
		{ColumnName: sampleSourceTimeColumn, DataType: schema.TypeTimestamp.String(), Required: true, PrimaryKey: true},
	})
	iotDataColumns = []string{sampleSourceDeviceColumn, sampleSourceNumberColumn, sampleSourceTimeColumn}
)

type IotData struct {
	table     string
	device    string
	number    int8
	eventTime time.Time
}

func (i *IotData) TableName() abstract.TableID {
	return abstract.TableID{
		Namespace: "",
		Name:      i.table,
	}
}

func (i *IotData) ToChangeItem(offset int64) abstract.ChangeItem {
	return abstract.ChangeItem{
		ID:          0,
		LSN:         uint64(offset),
		CommitTime:  uint64(i.eventTime.UnixNano()),
		Counter:     0,
		Kind:        abstract.InsertKind,
		Schema:      "",
		Table:       i.table,
		PartID:      "",
		TableSchema: iotDataSchema,
		ColumnNames: iotDataColumns,
		ColumnValues: []interface{}{
			i.device,
			i.number,
			i.eventTime,
		},
		OldKeys: abstract.EmptyOldKeys(),
		TxID:    "",
		Query:   "",
		Size:    abstract.RawEventSize(util.SizeOfStruct(*i) - uint64(len(i.table))),
	}
}

func NewIot(table string, device string, number int8, eventTime time.Time) *IotData {
	return &IotData{
		table:     table,
		device:    device,
		number:    number,
		eventTime: eventTime,
	}
}
