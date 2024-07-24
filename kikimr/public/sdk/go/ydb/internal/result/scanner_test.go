package result

import (
	"encoding/binary"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/internal"
	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

func valueFromPrimitiveTypeID(c *column) (*Ydb.Value, interface{}) {
	rv := rand.Int63()
	switch c.typeID {
	case Ydb.Type_BOOL:
		v := rv%2 == 1
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_BoolValue{
				BoolValue: v,
			},
		}
		if c.optional && !c.testDefault {
			vp := &v
			return ydbval, &vp
		}
		return ydbval, &v
	case Ydb.Type_INT8:
		v := int8(rv)
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_Int32Value{
				Int32Value: int32(v),
			},
		}
		if c.optional && !c.testDefault {
			vp := &v
			return ydbval, &vp
		}
		return ydbval, &v
	case Ydb.Type_UINT8:
		if c.nilValue {
			ydbval := &Ydb.Value{
				Value: &Ydb.Value_NullFlagValue{},
			}
			if c.testDefault {
				var dv uint8
				return ydbval, &dv
			}
			var dv *uint8
			return ydbval, &dv
		}
		v := uint8(rv)
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_Uint32Value{
				Uint32Value: uint32(v),
			},
		}
		if c.optional && !c.testDefault {
			vp := &v
			return ydbval, &vp
		}
		return ydbval, &v
	case Ydb.Type_INT16:
		v := int16(rv)
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_Int32Value{
				Int32Value: int32(v),
			},
		}
		if c.optional && !c.testDefault {
			vp := &v
			return ydbval, &vp
		}
		return ydbval, &v
	case Ydb.Type_UINT16:
		v := uint16(rv)
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_Uint32Value{
				Uint32Value: uint32(v),
			},
		}
		if c.optional && !c.testDefault {
			vp := &v
			return ydbval, &vp
		}
		return ydbval, &v
	case Ydb.Type_INT32:
		if c.nilValue {
			ydbval := &Ydb.Value{
				Value: &Ydb.Value_NullFlagValue{},
			}
			if c.testDefault {
				var dv int32
				return ydbval, &dv
			}
			var dv *int32
			return ydbval, &dv
		}
		v := int32(rv)
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_Int32Value{
				Int32Value: v,
			},
		}
		if c.optional && !c.testDefault {
			vp := &v
			return ydbval, &vp
		}
		return ydbval, &v
	case Ydb.Type_UINT32:
		v := uint32(rv)
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_Uint32Value{
				Uint32Value: v,
			},
		}
		if c.optional && !c.testDefault {
			vp := &v
			return ydbval, &vp
		}
		return ydbval, &v
	case Ydb.Type_INT64:
		v := rv
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_Int64Value{
				Int64Value: v,
			},
		}
		if c.ydbvalue {
			vp := ydb.Int64Value(v)
			return ydbval, &vp
		}
		if c.scanner {
			s := intIncScanner(v + 10)
			return ydbval, &s
		}
		if c.optional && !c.testDefault {
			vp := &v
			return ydbval, &vp
		}
		return ydbval, &v
	case Ydb.Type_UINT64:
		v := uint64(rv)
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_Uint64Value{
				Uint64Value: v,
			},
		}
		if c.optional && !c.testDefault {
			vp := &v
			return ydbval, &vp
		}
		return ydbval, &v
	case Ydb.Type_FLOAT:
		v := float32(rv)
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_FloatValue{
				FloatValue: v,
			},
		}
		if c.ydbvalue {
			vp := ydb.FloatValue(v)
			return ydbval, &vp
		}
		if c.optional && !c.testDefault {
			vp := &v
			return ydbval, &vp
		}
		return ydbval, &v
	case Ydb.Type_DOUBLE:
		v := float64(rv)
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_DoubleValue{
				DoubleValue: v,
			},
		}
		if c.optional && !c.testDefault {
			vp := &v
			return ydbval, &vp
		}
		return ydbval, &v
	case Ydb.Type_DATE:
		v := uint32(rv)
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_Uint32Value{
				Uint32Value: v,
			},
		}
		src := internal.UnmarshalDate(v)
		if c.scanner {
			s := dateScanner(src)
			return ydbval, &s
		}
		if c.optional && !c.testDefault {
			vp := &src
			return ydbval, &vp
		}
		return ydbval, &src
	case Ydb.Type_DATETIME:
		v := uint32(rv)
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_Uint32Value{
				Uint32Value: v,
			},
		}
		src := internal.UnmarshalDatetime(v)
		if c.optional && !c.testDefault {
			vp := &src
			return ydbval, &vp
		}
		return ydbval, &src
	case Ydb.Type_TIMESTAMP:
		v := uint64(rv)
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_Uint64Value{
				Uint64Value: v,
			},
		}
		src := internal.UnmarshalTimestamp(v)
		if c.optional && !c.testDefault {
			vp := &src
			return ydbval, &vp
		}
		return ydbval, &src
	case Ydb.Type_INTERVAL:
		if c.nilValue {
			ydbval := &Ydb.Value{
				Value: &Ydb.Value_NullFlagValue{},
			}
			if c.testDefault {
				var dv time.Duration
				return ydbval, &dv
			}
			var dv *time.Duration
			return ydbval, &dv
		}
		rv = rv % time.Now().Unix()
		v := rv
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_Int64Value{
				Int64Value: v,
			},
		}
		src := internal.DurationFromMicroseconds(v)
		if c.optional && !c.testDefault {
			vp := &src
			return ydbval, &vp
		}
		return ydbval, &src
	case Ydb.Type_TZ_DATE:
		rv = rv % (time.Now().Unix() / 24 / 60 / 60)
		v := internal.MarshalTzDate(internal.UnmarshalDate(uint32(rv)))
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_TextValue{
				TextValue: v,
			},
		}
		src, _ := internal.UnmarshalTzDate(v)
		if c.optional && !c.testDefault {
			vp := &src
			return ydbval, &vp
		}
		return ydbval, &src
	case Ydb.Type_TZ_DATETIME:
		if c.nilValue {
			ydbval := &Ydb.Value{
				Value: &Ydb.Value_NullFlagValue{},
			}
			if c.testDefault {
				var dv time.Time
				return ydbval, &dv
			}
			var dv *time.Time
			return ydbval, &dv
		}
		rv = rv % time.Now().Unix()
		v := internal.MarshalTzDatetime(internal.UnmarshalDatetime(uint32(rv)))
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_TextValue{
				TextValue: v,
			},
		}
		src, _ := internal.UnmarshalTzDatetime(v)
		if c.optional && !c.testDefault {
			vp := &src
			return ydbval, &vp
		}
		return ydbval, &src
	case Ydb.Type_TZ_TIMESTAMP:
		rv = rv % time.Now().Unix()
		v := internal.MarshalTzTimestamp(internal.UnmarshalTimestamp(uint64(rv)))
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_TextValue{
				TextValue: v,
			},
		}
		src, _ := internal.UnmarshalTzTimestamp(v)
		if c.optional && !c.testDefault {
			vp := &src
			return ydbval, &vp
		}
		return ydbval, &src
	case Ydb.Type_STRING:
		if c.nilValue {
			ydbval := &Ydb.Value{
				Value: &Ydb.Value_NullFlagValue{},
			}
			if c.scanner {
				return ydbval, &nullStringScanner{
					isNil: true,
				}
			}
			if !c.byteTest {
				if c.testDefault {
					var dv string
					return ydbval, &dv
				}
				var dv *string
				return ydbval, &dv
			} else {
				if c.testDefault {
					var dv [8]byte
					return ydbval, &dv
				}
				var dv *[8]byte
				return ydbval, &dv

			}
		}
		v := make([]byte, 8)
		binary.BigEndian.PutUint64(v, uint64(rv))
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_BytesValue{
				BytesValue: v,
			},
		}

		if c.scanner {
			return ydbval, &nullStringScanner{
				value: string(v),
				isNil: false,
			}
		}
		if !c.byteTest {
			src := string(v)
			if c.optional && !c.testDefault {
				vp := &src
				return ydbval, &vp
			}
			return ydbval, &src
		} else {
			var src [8]byte
			copy(src[:], v)
			if c.optional && !c.testDefault {
				vp := &src
				return ydbval, &vp
			}
			return ydbval, &src
		}
	case Ydb.Type_UTF8:
		v := strconv.FormatUint(uint64(rv), 10)
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_TextValue{
				TextValue: v,
			},
		}
		if c.optional && !c.testDefault {
			vp := &v
			return ydbval, &vp
		}
		return ydbval, &v
	case Ydb.Type_YSON:
		if c.nilValue {
			ydbval := &Ydb.Value{
				Value: &Ydb.Value_NullFlagValue{},
			}
			if c.testDefault {
				var dv []byte
				return ydbval, &dv
			}
			var dv *[]byte
			return ydbval, &dv
		}
		v := strconv.FormatUint(uint64(rv), 10)
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_TextValue{
				TextValue: v,
			},
		}
		src := []byte(v)
		if c.optional && !c.testDefault {
			vp := &src
			return ydbval, &vp
		}
		return ydbval, &src
	case Ydb.Type_JSON:
		v := strconv.FormatUint(uint64(rv), 10)
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_TextValue{
				TextValue: v,
			},
		}
		if c.ydbvalue {
			vp := ydb.JSONValue(v)
			return ydbval, &vp
		}
		src := []byte(v)
		if c.optional && !c.testDefault {
			vp := &src
			return ydbval, &vp
		}
		return ydbval, &src
	case Ydb.Type_UUID:
		if c.nilValue {
			ydbval := &Ydb.Value{
				Value: &Ydb.Value_NullFlagValue{},
			}
			if c.testDefault {
				var dv [16]byte
				return ydbval, &dv
			}
			var dv *[16]byte
			return ydbval, &dv
		}
		v := [16]byte{}
		binary.BigEndian.PutUint64(v[0:8], uint64(rv))
		binary.BigEndian.PutUint64(v[8:16], uint64(rv))
		ydbval := &Ydb.Value{
			High_128: binary.BigEndian.Uint64(v[0:8]),
			Value: &Ydb.Value_Low_128{
				Low_128: binary.BigEndian.Uint64(v[8:16]),
			},
		}
		if c.optional && !c.testDefault {
			vp := &v
			return ydbval, &vp
		}
		return ydbval, &v
	case Ydb.Type_JSON_DOCUMENT:
		v := strconv.FormatUint(uint64(rv), 10)
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_TextValue{
				TextValue: v,
			},
		}
		src := []byte(v)
		if c.optional && !c.testDefault {
			vp := &src
			return ydbval, &vp
		}
		return ydbval, &src
	case Ydb.Type_DYNUMBER:
		v := strconv.FormatUint(uint64(rv), 10)
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_TextValue{
				TextValue: v,
			},
		}
		if c.optional && !c.testDefault {
			vp := &v
			return ydbval, &vp
		}
		return ydbval, &v
	default:
		panic("ydb: unexpected type")
	}
}

func getResultSet(count int, col []*column) (r *Ydb.ResultSet, testValues [][]interface{}) {
	r = &Ydb.ResultSet{}
	for _, c := range col {
		t := &Ydb.Type{
			Type: &Ydb.Type_TypeId{
				TypeId: c.typeID,
			},
		}
		if c.optional {
			t = &Ydb.Type{
				Type: &Ydb.Type_OptionalType{
					OptionalType: &Ydb.OptionalType{
						Item: t,
					},
				},
			}
		}
		r.Columns = append(r.Columns, &Ydb.Column{
			Name: c.name,
			Type: t})
	}

	testValues = make([][]interface{}, count)
	for i := 0; i < count; i++ {
		var items []*Ydb.Value
		var vals []interface{}
		for j := range r.Columns {
			v, val := valueFromPrimitiveTypeID(col[j])
			vals = append(vals, val)
			items = append(items, v)
		}
		r.Rows = append(r.Rows, &Ydb.Value{
			Items: items,
		})
		testValues[i] = vals
	}
	return r, testValues
}

func TestScanSqlTypes(t *testing.T) {
	s := initScanner()
	for _, test := range scannerData {
		set, expected := getResultSet(test.count, test.columns)
		Reset(s, set, test.setColumns...)
		var err error
		for s.NextRow() {
			if test.columns[0].testDefault {
				err = s.ScanWithDefaults(test.values...)
			} else {
				err = s.Scan(test.values...)
			}
			if err != nil {
				t.Fatalf("test: %s; error: %s", test.name, err)
			}
			ok := true
			if test.setColumnIndexes != nil {
				for i, v := range test.setColumnIndexes {
					ok = assert.Equal(t, expected[0][v], test.values[i])
				}
			} else {
				ok = assert.Equal(t, expected[0], test.values)
			}
			if ok == false {
				t.Fatalf("test: %s; error: not equal", test.name)
			}
			expected = expected[1:]
		}
	}

}
