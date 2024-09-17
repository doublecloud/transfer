package tostring

import (
	"testing"
	"time"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/test/canon"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/transformer/registry/filter"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/schema"
)

func TestToStringTransformer(t *testing.T) {
	t.Parallel()
	tableF, _ := filter.NewFilter(nil, []string{})
	colF, _ := filter.NewFilter([]string{}, nil)
	allToStringTransformer := ToStringTransformer{
		Tables:  tableF,
		Columns: colF,
		Logger:  logger.Log,
	}

	tableF, _ = filter.NewFilter([]string{"db.table"}, nil)
	colF, _ = filter.NewFilter([]string{}, []string{"column2"})
	excludeColTransformer := ToStringTransformer{
		Tables:  tableF,
		Columns: colF,
		Logger:  logger.Log,
	}

	tableF, _ = filter.NewFilter([]string{"db.a_table3"}, nil)
	colF, _ = filter.NewFilter([]string{"column1", "column3"}, nil)
	includeTwoColsTransformer := ToStringTransformer{
		Tables:  tableF,
		Columns: colF,
		Logger:  logger.Log,
	}

	table1 := abstract.TableID{
		Namespace: "db",
		Name:      "table1",
	}

	table2 := abstract.TableID{
		Namespace: "db",
		Name:      "table2",
	}

	table3 := abstract.TableID{
		Namespace: "db",
		Name:      "a_table3",
	}

	table1Schema := abstract.NewTableSchema(abstract.TableColumns{
		abstract.MakeTypedColSchema("column1", string(schema.TypeString), true),
		abstract.MakeTypedColSchema("column2", string(schema.TypeInt64), false),
		abstract.MakeTypedColSchema("column3", string(schema.TypeInt32), false),
		abstract.MakeTypedColSchema("column4", string(schema.TypeBoolean), false),
	})

	table2Schema := abstract.NewTableSchema(abstract.TableColumns{
		abstract.MakeTypedColSchema("column1", string(schema.TypeBytes), false),
		abstract.MakeTypedColSchema("column2", string(schema.TypeDate), false),
		abstract.MakeTypedColSchema("column3", string(schema.TypeFloat64), false),
		abstract.MakeTypedColSchema("column4", string(schema.TypeFloat32), false),
	})

	table3Schema := abstract.NewTableSchema(abstract.TableColumns{
		abstract.MakeTypedColSchema("column2", string(schema.TypeInt8), false),
		abstract.MakeTypedColSchema("column3", string(schema.TypeUint32), false),
		abstract.MakeTypedColSchema("column4", string(schema.TypeDate), false),
	})

	require.True(t, allToStringTransformer.Suitable(table1, table1Schema))
	require.True(t, allToStringTransformer.Suitable(table2, table2Schema))
	require.True(t, allToStringTransformer.Suitable(table3, table3Schema))

	require.True(t, excludeColTransformer.Suitable(table1, table1Schema))
	require.True(t, excludeColTransformer.Suitable(table2, table2Schema))
	require.False(t, excludeColTransformer.Suitable(table3, table3Schema))

	require.False(t, includeTwoColsTransformer.Suitable(table1, table1Schema))
	require.False(t, includeTwoColsTransformer.Suitable(table2, table2Schema))
	require.True(t, includeTwoColsTransformer.Suitable(table3, table3Schema))

	item1 := abstract.ChangeItem{
		Kind:         "Insert",
		Schema:       "db",
		Table:        "table1",
		ColumnNames:  []string{"column1", "column2", "column3", "column4"},
		ColumnValues: []interface{}{"value1", int64(123), int32(1234), true},
		TableSchema:  table1Schema,
	}

	item2 := abstract.ChangeItem{
		Kind:        "Insert",
		Schema:      "db",
		Table:       "table2",
		ColumnNames: []string{"colunm1", "column2", "column3", "column4"},
		ColumnValues: []interface{}{
			"value1",
			time.Date(1703, 1, 2, 0, 0, 0, 0, time.UTC),
			123.123,
			float32(312.321)},
		TableSchema: table2Schema,
	}
	item3 := abstract.ChangeItem{
		Kind:         "Insert",
		Schema:       "db",
		Table:        "a_table3",
		ColumnNames:  []string{"column2", "column3", "column4"},
		ColumnValues: []interface{}{int8(-3), uint32(12345), time.Minute},
		TableSchema:  table3Schema,
	}
	var result []abstract.TransformerResult
	for _, transformer := range []ToStringTransformer{allToStringTransformer, excludeColTransformer, includeTwoColsTransformer} {
		for _, item := range []abstract.ChangeItem{item1, item2, item3} {
			if transformer.Suitable(item.TableID(), item.TableSchema) {
				result = append(result, transformer.Apply([]abstract.ChangeItem{item}))
			}
		}
	}

	canon.SaveJSON(t, result)
}

func TestAllTypesToStringTransformer(t *testing.T) {
	t.Parallel()
	var testCases = []struct {
		originalValue interface{}
		originalType  schema.Type
		expectedValue string
	}{
		// check not nulls
		{[]interface{}{1, "string", 3, 4.123, 6, true}, schema.TypeAny, `[1,"string",3,4.123,6,true]`},
		{map[string]interface{}{"someName": "someValue", "someName2": 1234}, schema.TypeAny, `{"someName":"someValue","someName2":1234}`},
		{int64(981274987), schema.TypeInt64, "981274987"},
		{int32(-12049182), schema.TypeInt32, "-12049182"},
		{int16(12313), schema.TypeInt16, "12313"},
		{int8(-14), schema.TypeInt8, "-14"},
		{uint64(1142423562), schema.TypeUint64, "1142423562"},
		{uint32(0), schema.TypeUint32, "0"},
		{uint16(65212), schema.TypeUint16, "65212"},
		{uint8(213), schema.TypeUint8, "213"},
		{float32(123.123), schema.TypeFloat32, "123.123"},
		{float64(-12344.12334341), schema.TypeFloat64, "-12344.12334341"},
		{[]byte("bytes"), schema.TypeBytes, "bytes"},
		{"string", schema.TypeString, "string"},
		{true, schema.TypeBoolean, "true"},
		{time.Date(-1232, 2, 23, 0, 0, 0, 0, time.UTC), schema.TypeDate, "-1232-02-23"},
		{time.Date(14124, 1, 12, 0, 0, 0, 0, time.UTC), schema.TypeDate, "14124-01-12"},
		{time.Date(2311, 12, 1, 1, 2, 4, 5, time.UTC), schema.TypeDatetime, "2311-12-01T01:02:04.000000005Z"},
		{time.Date(1231, 5, 23, 9, 8, 7, 6, time.UTC), schema.TypeTimestamp, "1231-05-23T09:08:07.000000006Z"},
		{time.Duration(12*time.Hour + 53*time.Minute + 21*time.Second + 87*time.Millisecond + 182*time.Microsecond + 124*time.Nanosecond), schema.TypeInterval, "12h53m21.087182124s"},
		// check nulls
		{nil, schema.TypeDate, "<nil>"},
		{nil, schema.TypeDatetime, "<nil>"},
		{nil, schema.TypeBoolean, "<nil>"},
		{nil, schema.TypeString, "<nil>"},
		{nil, schema.TypeInt64, "<nil>"},
	}
	for _, testCase := range testCases {
		require.Equal(t, testCase.expectedValue, SerializeToString(testCase.originalValue, testCase.originalType.String()))
	}
}
