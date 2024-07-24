package sample

import (
	"time"

	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/util"
	"go.ytsaurus.tech/yt/go/schema"
)

const (
	sampleSourceUserColumn = "user_logins"
	sampleSourceCityColumn = "city"
)

var (
	_ StreamingData = (*UserLogins)(nil)

	userLoginsColumnSchema = abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: sampleSourceUserColumn, DataType: schema.TypeString.String(), Required: true, PrimaryKey: true},
		{ColumnName: sampleSourceCityColumn, DataType: schema.TypeString.String(), Required: true},
		{ColumnName: sampleSourceTimeColumn, DataType: schema.TypeTimestamp.String(), Required: true, PrimaryKey: true},
	})

	userLoginColumns = []string{sampleSourceUserColumn, sampleSourceCityColumn, sampleSourceTimeColumn}
)

type UserLogins struct {
	table     string
	user      string
	city      string
	eventTime time.Time
}

func (u *UserLogins) TableName() abstract.TableID {
	return abstract.TableID{
		Namespace: "",
		Name:      u.table,
	}
}

func (u *UserLogins) ToChangeItem(offset int64) abstract.ChangeItem {
	return abstract.ChangeItem{
		ID:          0,
		LSN:         uint64(offset),
		CommitTime:  uint64(u.eventTime.UnixNano()),
		Counter:     0,
		Kind:        abstract.InsertKind,
		Schema:      "",
		Table:       u.table,
		PartID:      "",
		TableSchema: userLoginsColumnSchema,
		ColumnNames: userLoginColumns,
		ColumnValues: []interface{}{
			u.user,
			u.city,
			u.eventTime,
		},
		OldKeys: abstract.EmptyOldKeys(),
		TxID:    "",
		Query:   "",
		// removing table string size because it is not added in column values
		Size: abstract.RawEventSize(util.SizeOfStruct(*u) - uint64(len(u.table))),
	}
}

func NewUserLogins(table string, user string, city string, eventTime time.Time) *UserLogins {
	return &UserLogins{
		table:     table,
		user:      user,
		city:      city,
		eventTime: eventTime,
	}
}
