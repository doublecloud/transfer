package sink

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/internal/metrics"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/coordinator"
	yt2 "github.com/doublecloud/tross/transfer_manager/go/pkg/providers/yt"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
)

var bigRowSchema = abstract.NewTableSchema([]abstract.ColSchema{
	{DataType: string(schema.TypeInt8), ColumnName: "MyInt8", PrimaryKey: false},
	{DataType: string(schema.TypeInt16), ColumnName: "MyInt16", PrimaryKey: false},
	{DataType: string(schema.TypeInt32), ColumnName: "MyInt32", PrimaryKey: false},
	{DataType: string(schema.TypeInt64), ColumnName: "MyInt64", PrimaryKey: true},
	{DataType: string(schema.TypeUint8), ColumnName: "MyUint8", PrimaryKey: false},
	{DataType: string(schema.TypeUint16), ColumnName: "MyUint16", PrimaryKey: false},
	{DataType: string(schema.TypeUint32), ColumnName: "MyUint32", PrimaryKey: false},
	{DataType: string(schema.TypeUint64), ColumnName: "MyUint64", PrimaryKey: false},
	{DataType: string(schema.TypeFloat32), ColumnName: "MyFloat", PrimaryKey: false},
	{DataType: string(schema.TypeFloat64), ColumnName: "MyDouble", PrimaryKey: false},
	{DataType: string(schema.TypeBytes), ColumnName: "MyBytes", PrimaryKey: false},
	{DataType: string(schema.TypeString), ColumnName: "MyString", PrimaryKey: false},
	{DataType: string(schema.TypeBoolean), ColumnName: "MyBoolean", PrimaryKey: false},
	{DataType: string(schema.TypeAny), ColumnName: "MyAny", PrimaryKey: false},
})

type bigRow struct {
	MyInt8    int8        `yson:"MyInt8"`
	MyInt16   int16       `yson:"MyInt16"`
	MyInt32   int32       `yson:"MyInt32"`
	MyInt64   int64       `yson:"MyInt64"`
	MyUint8   uint8       `yson:"MyUint8"`
	MyUint16  uint16      `yson:"MyUint16"`
	MyUint32  uint32      `yson:"MyUint32"`
	MyUint64  uint64      `yson:"MyUint64"`
	MyFloat   float32     `yson:"MyFloat"`
	MyDouble  float64     `yson:"MyDouble"`
	MyBytes   []byte      `yson:"MyBytes"`
	MyString  string      `yson:"MyString"`
	MyBoolean bool        `yson:"MyBoolean"`
	MyAny     interface{} `yson:"MyAny"`
}

func newBigRow() bigRow {
	var f bigRow
	_ = gofakeit.Struct(&f)
	return f
}

func (b *bigRow) toValues() []interface{} {
	return []interface{}{
		b.MyInt8,
		b.MyInt16,
		b.MyInt32,
		b.MyInt64,
		b.MyUint8,
		b.MyUint16,
		b.MyUint32,
		b.MyUint64,
		b.MyFloat,
		b.MyDouble,
		b.MyBytes,
		b.MyString,
		b.MyBoolean,
		b.MyAny,
	}
}

func (b *bigRow) toChangeItem(namespace, name string) abstract.ChangeItem {
	return abstract.ChangeItem{
		TableSchema:  bigRowSchema,
		Kind:         abstract.InsertKind,
		Schema:       namespace,
		Table:        name,
		ColumnNames:  bigRowSchema.Columns().ColumnNames(),
		ColumnValues: b.toValues(),
	}
}

func TestStaticTable(t *testing.T) {
	t.Run("simple test", staticTableSimple)
	t.Run("wrong schema test", wrongOrderOfValuesInChangeItem)
	t.Run("custom attributes test", TestCustomAttributesStaticTable)
}

func staticTableSimple(t *testing.T) {
	var err error
	path := ypath.Path("//home/cdc/test/TM-3788/staticTableSimple")
	// create single static table for change item consumption
	cfg, ytClient := initYt(t, path.String())
	defer ytClient.Stop()
	defer teardown(ytClient, path)
	// schema might be unknown during initialization
	tableID := abstract.TableID{
		Namespace: "ns",
		Name:      "weird_table",
	}
	statTable := NewStaticTableFromConfig(ytClient, cfg, metrics.NewRegistry(), logger.Log, coordinator.NewStatefulFakeClient(), "dtt-test1")

	// generate some amount of random change items
	data := []bigRow{}
	items := []abstract.ChangeItem{}
	for i := 0; i < 79; i++ {
		row := newBigRow()
		data = append(data, row)
		items = append(items, row.toChangeItem(tableID.Namespace, tableID.Name))
	}
	// push initial items
	err = statTable.Push([]abstract.ChangeItem{{
		TableSchema: bigRowSchema,
		Kind:        abstract.InitShardedTableLoad,
		Schema:      tableID.Namespace,
		Table:       tableID.Name,
	}})
	require.NoError(t, err)
	err = statTable.Push([]abstract.ChangeItem{{
		TableSchema: bigRowSchema,
		Kind:        abstract.InitTableLoad,
		Schema:      tableID.Namespace,
		Table:       tableID.Name,
	}})
	require.NoError(t, err)
	// write change items
	err = statTable.Push(items)
	require.NoError(t, err)
	// push final items
	err = statTable.Push([]abstract.ChangeItem{{
		TableSchema: bigRowSchema,
		Kind:        abstract.DoneTableLoad,
		Schema:      tableID.Namespace,
		Table:       tableID.Name,
	}})
	require.NoError(t, err)
	err = statTable.Push([]abstract.ChangeItem{{
		TableSchema: bigRowSchema,
		Kind:        abstract.DoneShardedTableLoad,
		Schema:      tableID.Namespace,
		Table:       tableID.Name,
	}})
	require.NoError(t, err)
	err = statTable.Close()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// check what nodes do we have
	var listNodeResult []struct {
		Name string `yson:",value"`
	}
	directoryNode := ypath.Path(cfg.Path())
	err = ytClient.ListNode(ctx, directoryNode, &listNodeResult, nil)
	logger.Log.Info("List of table in destination folder", log.Any("list", listNodeResult))
	require.NoError(t, err)
	require.Len(t, listNodeResult, 1, "there should be only one child")

	tableNode := yt2.SafeChild(directoryNode, listNodeResult[0].Name)
	// load result from YT
	rows, err := ytClient.ReadTable(ctx, tableNode.YPath(), nil)
	require.NoError(t, err)
	var res []bigRow
	for rows.Next() {
		var row bigRow
		require.NoError(t, rows.Scan(&row))
		res = append(res, row)
	}
	// sort answer to preserve order
	sort.Slice(data, func(i, j int) bool {
		return data[i].MyInt64 < data[j].MyInt64
	})
	sort.Slice(res, func(i, j int) bool {
		return res[i].MyInt64 < res[j].MyInt64
	})
	require.Equal(t, data, res)
}

func wrongOrderOfValuesInChangeItem(t *testing.T) {
	var err error
	path := ypath.Path("//home/cdc/test/TM-3788/wrongOrderOfValuesInChangeItem")
	// create single static table for change item consumption
	cfg, ytClient := initYt(t, path.String())
	defer ytClient.Stop()
	defer teardown(ytClient, path)
	// schema might be unknown during initialization
	tableID := abstract.TableID{
		Namespace: "ns",
		Name:      "weird_table_2",
	}
	statTable := NewStaticTableFromConfig(ytClient, cfg, metrics.NewRegistry(), logger.Log, coordinator.NewStatefulFakeClient(), "dtt-test2")

	// push initial item
	err = statTable.Push([]abstract.ChangeItem{{
		TableSchema: bigRowSchema,
		Kind:        abstract.InitShardedTableLoad,
		Schema:      tableID.Namespace,
		Table:       tableID.Name,
	}})
	require.NoError(t, err)
	err = statTable.Push([]abstract.ChangeItem{{
		TableSchema: bigRowSchema,
		Kind:        abstract.InitTableLoad,
		Schema:      tableID.Namespace,
		Table:       tableID.Name,
	}})
	require.NoError(t, err)
	// write wrong change item (not compliant to scheme)
	row := newBigRow()
	values := row.toValues()
	values[3] = false
	err = statTable.Push([]abstract.ChangeItem{
		{
			TableSchema:  bigRowSchema,
			Kind:         abstract.InsertKind,
			Schema:       tableID.Namespace,
			Table:        tableID.Name,
			ColumnNames:  bigRowSchema.Columns().ColumnNames(),
			ColumnValues: values,
		}})
	require.NoError(t, err)
	err = statTable.Push([]abstract.ChangeItem{{
		TableSchema: bigRowSchema,
		Kind:        abstract.DoneTableLoad,
		Schema:      tableID.Namespace,
		Table:       tableID.Name,
	}})
	require.ErrorContains(t, err, "invalid type: expected \"int64\", actual \"boolean\"")
}

func TestCustomAttributesStaticTable(t *testing.T) {
	path := ypath.Path("//home/cdc/test/static/test_table")
	// create single static table for change item consumption
	cfg, ytClient := initYt(t, path.String())
	defer ytClient.Stop()
	defer teardown(ytClient, path)
	// schema might be unknown during initialization
	tableID := abstract.TableID{
		Namespace: "ns",
		Name:      "weird_table_2",
	}
	statTable, err := NewStaticSink(cfg, metrics.NewRegistry(), logger.Log, coordinator.NewFakeClient(), "test_transfer")
	require.NoError(t, err)
	// generate some amount of random change items
	var items []abstract.ChangeItem
	for i := 0; i < 1; i++ {
		row := newBigRow()
		items = append(items, row.toChangeItem(tableID.Namespace, tableID.Name))
	}
	// push initial items
	require.NoError(t, statTable.Push([]abstract.ChangeItem{{
		TableSchema: bigRowSchema,
		Kind:        abstract.InitShardedTableLoad,
		Schema:      tableID.Namespace,
		Table:       tableID.Name,
	}}))
	require.NoError(t, statTable.Push([]abstract.ChangeItem{{
		TableSchema: bigRowSchema,
		Kind:        abstract.InitTableLoad,
		Schema:      tableID.Namespace,
		Table:       tableID.Name,
	}}))
	// write change items
	require.NoError(t, statTable.Push(items))
	// push final items
	require.NoError(t, statTable.Push([]abstract.ChangeItem{{
		TableSchema: bigRowSchema,
		Kind:        abstract.DoneTableLoad,
		Schema:      tableID.Namespace,
		Table:       tableID.Name,
	}}))
	require.NoError(t, statTable.Push([]abstract.ChangeItem{{
		TableSchema: bigRowSchema,
		Kind:        abstract.DoneShardedTableLoad,
		Schema:      tableID.Namespace,
		Table:       tableID.Name,
	}}))
	var attr bool
	require.NoError(t, ytClient.GetNode(context.Background(), ypath.Path("//home/cdc/test/static/test_table/ns_weird_table_2/@test"), &attr, nil))
	require.Equal(t, true, attr)
}
