package sample

import (
	"time"

	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/util"
	"go.ytsaurus.tech/yt/go/schema"
)

const (
	sampleSourceHostnameColumn    = "hostname"
	sampleSourceRegionColumn      = "region"
	sampleSourceUsageUserColumn   = "usage_user"
	sampleSourceUsageSystemColumn = "usage_system"
)

var (
	_ StreamingData = (*Devops)(nil)

	devopsColumnsSchema = abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: sampleSourceHostnameColumn, DataType: schema.TypeString.String(), Required: true, PrimaryKey: true},
		{ColumnName: sampleSourceRegionColumn, DataType: schema.TypeString.String(), Required: true, PrimaryKey: true},
		{ColumnName: sampleSourceUsageUserColumn, DataType: schema.TypeInt8.String(), Required: true},
		{ColumnName: sampleSourceUsageSystemColumn, DataType: schema.TypeInt8.String(), Required: true},
		{ColumnName: sampleSourceTimeColumn, DataType: schema.TypeTimestamp.String(), Required: true, PrimaryKey: true},
	})

	devopsColumns = []string{sampleSourceHostnameColumn, sampleSourceRegionColumn, sampleSourceUsageUserColumn, sampleSourceUsageSystemColumn, sampleSourceTimeColumn}
)

type Devops struct {
	table       string
	hostname    string
	region      string
	usageUser   int8
	usageSystem int8
	eventTime   time.Time
}

func (d *Devops) TableName() abstract.TableID {
	return abstract.TableID{
		Namespace: "",
		Name:      d.table,
	}
}

func (d *Devops) ToChangeItem(offset int64) abstract.ChangeItem {
	return abstract.ChangeItem{
		ID:          0,
		LSN:         uint64(offset),
		CommitTime:  uint64(d.eventTime.UnixNano()),
		Counter:     0,
		Kind:        abstract.InsertKind,
		Schema:      "",
		Table:       d.table,
		PartID:      "",
		TableSchema: devopsColumnsSchema,
		ColumnNames: devopsColumns,
		ColumnValues: []interface{}{
			d.hostname,
			d.region,
			d.usageUser,
			d.usageSystem,
			d.eventTime,
		},
		OldKeys: abstract.EmptyOldKeys(),
		TxID:    "",
		Query:   "",
		Size:    abstract.RawEventSize(util.SizeOfStruct(*d) - uint64(len(d.table))),
	}
}

func NewDevops(table string, hostname string, region string, usageUser int8, usageSystem int8, eventTime time.Time) *Devops {
	return &Devops{
		table:       table,
		hostname:    hostname,
		region:      region,
		usageUser:   usageUser,
		usageSystem: usageSystem,
		eventTime:   eventTime,
	}
}
