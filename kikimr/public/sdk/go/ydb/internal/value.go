package internal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

type V interface {
	toYDB() *Ydb.TypedValue
	toString(*bytes.Buffer)
}

func ValueToYDB(v V) *Ydb.TypedValue {
	return v.toYDB()
}

func ValueFromYDB(t *Ydb.Type, v *Ydb.Value) V {
	return Value{
		t: TypeFromYDB(t),
		v: v,
	}
}

func WriteValueStringTo(buf *bytes.Buffer, v V) {
	v.toString(buf)
}

// BigEndianUint128 builds a big-endian uint128 value.
func BigEndianUint128(hi, lo uint64) (v [16]byte) {
	binary.BigEndian.PutUint64(v[0:8], hi)
	binary.BigEndian.PutUint64(v[8:16], lo)
	return v
}

// PrimitiveFromYDB returns a primitive value stored in x.
// Currently it may return one of this types:
//
//	bool
//	int32
//	uint32
//	int64
//	uint64
//	float32
//	float64
//	[]byte
//	string
//	[16]byte
//
// Or nil.
func PrimitiveFromYDB(x *Ydb.Value) (v interface{}) {
	if x != nil {
		v, _ = primitiveFromYDB(x)
	}
	return v
}

func primitiveFromYDB(x *Ydb.Value) (v interface{}, primitive bool) {
	switch v := x.Value.(type) {
	case *Ydb.Value_BoolValue:
		return v.BoolValue, true
	case *Ydb.Value_Int32Value:
		return v.Int32Value, true
	case *Ydb.Value_Uint32Value:
		return v.Uint32Value, true
	case *Ydb.Value_Int64Value:
		return v.Int64Value, true
	case *Ydb.Value_Uint64Value:
		return v.Uint64Value, true
	case *Ydb.Value_FloatValue:
		return v.FloatValue, true
	case *Ydb.Value_DoubleValue:
		return v.DoubleValue, true
	case *Ydb.Value_BytesValue:
		return v.BytesValue, true
	case *Ydb.Value_TextValue:
		return v.TextValue, true
	case *Ydb.Value_Low_128:
		return BigEndianUint128(x.High_128, v.Low_128), true
	case *Ydb.Value_NullFlagValue:
		return nil, true
	default:
		return nil, false
	}
}

type Value struct {
	t T
	v *Ydb.Value
}

func (v Value) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

func valueToString(buf *bytes.Buffer, t T, v *Ydb.Value) {
	buf.WriteByte('(')
	defer buf.WriteByte(')')
	if x, ok := primitiveFromYDB(v); ok {
		if x != nil {
			fmt.Fprintf(buf, "%v", x)
		} else {
			buf.WriteString("NULL")
		}
		return
	}
	if x, ok := v.Value.(*Ydb.Value_NestedValue); ok {
		switch x := t.(type) {
		case VariantType:
			var (
				i = int(v.VariantIndex)
				s string
			)
			if !x.S.Empty() {
				f := x.S.Fields[i]
				t = f.Type
				s = f.Name
			} else {
				t = x.T.Elems[i]
				s = strconv.Itoa(i)
			}
			buf.WriteString(s)
			buf.WriteByte('=')

		case OptionalType:
			t = x.T

		default:
			panic("ydb: unknown nested type")
		}
		valueToString(buf, t, x.NestedValue)
		return
	}
	if n := len(v.Items); n > 0 {
		types := make([]T, n)
		switch x := t.(type) {
		case StructType:
			for i, f := range x.Fields {
				types[i] = f.Type
			}
		case ListType:
			for i := range types {
				types[i] = x.T
			}
		case TupleType:
			copy(types, x.Elems)
		default:
			panic("ydb: unkown iterable type")
		}
		for i, item := range v.Items {
			valueToString(buf, types[i], item)
		}
		return
	}
	if len(v.Pairs) > 0 {
		dict := t.(DictType)
		for _, pair := range v.Pairs {
			buf.WriteByte('(')
			valueToString(buf, dict.Key, pair.Key)
			valueToString(buf, dict.Payload, pair.Payload)
			buf.WriteByte(')')
		}
	}
}

func (v Value) toString(buf *bytes.Buffer) {
	v.t.toString(buf)
	valueToString(buf, v.t, v.v)
}

func (v Value) toYDB() *Ydb.TypedValue {
	// TODO: may be optimized -1 allocation: make all *Value() methods return
	// *Value, put TypedValue, Value and Type on Value and then use pointer to
	// already heap-allocated bytes.
	return &Ydb.TypedValue{
		Type:  v.t.toYDB(),
		Value: v.v,
	}
}

func BoolValue(v bool) Value {
	return Value{
		t: TypeBool,
		v: &Ydb.Value{
			Value: &Ydb.Value_BoolValue{
				BoolValue: v,
			},
		},
	}
}
func Int8Value(v int8) Value {
	return Value{
		t: TypeInt8,
		v: &Ydb.Value{
			Value: &Ydb.Value_Int32Value{
				Int32Value: int32(v),
			},
		},
	}
}
func Uint8Value(v uint8) Value {
	return Value{
		t: TypeUint8,
		v: &Ydb.Value{
			Value: &Ydb.Value_Uint32Value{
				Uint32Value: uint32(v),
			},
		},
	}
}
func Int16Value(v int16) Value {
	return Value{
		t: TypeInt16,
		v: &Ydb.Value{
			Value: &Ydb.Value_Int32Value{
				Int32Value: int32(v),
			},
		},
	}
}
func Uint16Value(v uint16) Value {
	return Value{
		t: TypeUint16,
		v: &Ydb.Value{
			Value: &Ydb.Value_Uint32Value{
				Uint32Value: uint32(v),
			},
		},
	}
}
func Int32Value(v int32) Value {
	return Value{
		t: TypeInt32,
		v: &Ydb.Value{
			Value: &Ydb.Value_Int32Value{
				Int32Value: v,
			},
		},
	}
}
func Uint32Value(v uint32) Value {
	return Value{
		t: TypeUint32,
		v: &Ydb.Value{
			Value: &Ydb.Value_Uint32Value{
				Uint32Value: v,
			},
		},
	}
}
func Int64Value(v int64) Value {
	return Value{
		t: TypeInt64,
		v: &Ydb.Value{
			Value: &Ydb.Value_Int64Value{
				Int64Value: v,
			},
		},
	}
}
func Uint64Value(v uint64) Value {
	return Value{
		t: TypeUint64,
		v: &Ydb.Value{
			Value: &Ydb.Value_Uint64Value{
				Uint64Value: v,
			},
		},
	}
}
func FloatValue(v float32) Value {
	return Value{
		t: TypeFloat,
		v: &Ydb.Value{
			Value: &Ydb.Value_FloatValue{
				FloatValue: v,
			},
		},
	}
}
func DoubleValue(v float64) Value {
	return Value{
		t: TypeDouble,
		v: &Ydb.Value{
			Value: &Ydb.Value_DoubleValue{
				DoubleValue: v,
			},
		},
	}
}
func DateValue(v uint32) Value {
	return Value{
		t: TypeDate,
		v: &Ydb.Value{
			Value: &Ydb.Value_Uint32Value{
				Uint32Value: v,
			},
		},
	}
}
func DatetimeValue(v uint32) Value {
	return Value{
		t: TypeDatetime,
		v: &Ydb.Value{
			Value: &Ydb.Value_Uint32Value{
				Uint32Value: v,
			},
		},
	}
}
func TimestampValue(v uint64) Value {
	return Value{
		t: TypeTimestamp,
		v: &Ydb.Value{
			Value: &Ydb.Value_Uint64Value{
				Uint64Value: v,
			},
		},
	}
}
func IntervalValue(v int64) Value {
	return Value{
		t: TypeInterval,
		v: &Ydb.Value{
			Value: &Ydb.Value_Int64Value{
				Int64Value: v,
			},
		},
	}
}
func TzDateValue(v string) Value {
	return Value{
		t: TypeTzDate,
		v: &Ydb.Value{
			Value: &Ydb.Value_TextValue{
				TextValue: v,
			},
		},
	}
}
func TzDatetimeValue(v string) Value {
	return Value{
		t: TypeTzDatetime,
		v: &Ydb.Value{
			Value: &Ydb.Value_TextValue{
				TextValue: v,
			},
		},
	}
}
func TzTimestampValue(v string) Value {
	return Value{
		t: TypeTzTimestamp,
		v: &Ydb.Value{
			Value: &Ydb.Value_TextValue{
				TextValue: v,
			},
		},
	}
}
func StringValue(v []byte) Value {
	return Value{
		t: TypeString,
		v: &Ydb.Value{
			Value: &Ydb.Value_BytesValue{
				BytesValue: v,
			},
		},
	}
}
func UTF8Value(v string) Value {
	return Value{
		t: TypeUTF8,
		v: &Ydb.Value{
			Value: &Ydb.Value_TextValue{
				TextValue: v,
			},
		},
	}
}
func YSONValue(v []byte) Value {
	return Value{
		t: TypeYSON,
		v: &Ydb.Value{
			Value: &Ydb.Value_BytesValue{
				BytesValue: v,
			},
		},
	}
}
func JSONValue(v string) Value {
	return Value{
		t: TypeJSON,
		v: &Ydb.Value{
			Value: &Ydb.Value_TextValue{
				TextValue: v,
			},
		},
	}
}
func UUIDValue(v [16]byte) Value {
	return Value{
		t: TypeUUID,
		v: &Ydb.Value{
			High_128: binary.BigEndian.Uint64(v[0:8]),
			Value: &Ydb.Value_Low_128{
				Low_128: binary.BigEndian.Uint64(v[8:16]),
			},
		},
	}
}
func JSONDocumentValue(v string) Value {
	return Value{
		t: TypeJSONDocument,
		v: &Ydb.Value{
			Value: &Ydb.Value_TextValue{
				TextValue: v,
			},
		},
	}
}
func DyNumberValue(v string) Value {
	return Value{
		t: TypeDyNumber,
		v: &Ydb.Value{
			Value: &Ydb.Value_TextValue{
				TextValue: v,
			},
		},
	}
}
func DecimalValue(t T, v [16]byte) Value {
	return Value{
		t: t,
		v: &Ydb.Value{
			High_128: binary.BigEndian.Uint64(v[0:8]),
			Value: &Ydb.Value_Low_128{
				Low_128: binary.BigEndian.Uint64(v[8:16]),
			},
		},
	}
}

var VoidValue = Value{
	t: VoidType{},
	v: &Ydb.Value{
		Value: new(Ydb.Value_NullFlagValue),
	},
}

func TupleValue(n int, it func(int) V) Value {
	var (
		types = make([]T, n)
		items = make([]*Ydb.Value, n)
	)
	for i := 0; i < n; i++ {
		types[i] = it(i).(Value).t
		items[i] = it(i).(Value).v
	}
	return Value{
		t: TupleType{types},
		v: &Ydb.Value{
			Items: items,
		},
	}
}

type StructValueProto struct {
	Fields []StructField
	Values []*Ydb.Value
	//TODO: proto reuse ability – maintain an index of fieldname -> int (for
	//Set(name, value) method).
}

func (s *StructValueProto) Add(name string, value V) {
	s.Fields = append(s.Fields, StructField{
		Name: name,
		Type: value.(Value).t,
	})
	s.Values = append(s.Values, value.(Value).v)
}

func StructValue(p *StructValueProto) Value {
	return Value{
		t: StructType{p.Fields},
		v: &Ydb.Value{
			Items: p.Values,
		},
	}
}

func DictValue(n int, it func(int) V) Value {
	if n == 0 || n%2 == 1 {
		panic("malformed number of pairs")
	}
	var (
		keyT     = it(0).(Value).t
		payloadT = it(1).(Value).t
	)
	ps := make([]*Ydb.ValuePair, n/2)
	for i := 0; i < n; i += 2 {
		k := it(i).(Value)
		p := it(i + 1).(Value)
		if !TypesEqual(k.t, keyT) {
			panic(fmt.Sprintf(
				"unexpected key type: %s; want %s",
				k.t, keyT,
			))
		}
		if !TypesEqual(p.t, payloadT) {
			panic(fmt.Sprintf(
				"unexpected payload type: %s; want %s",
				p.t, payloadT,
			))
		}
		ps[i/2] = &Ydb.ValuePair{
			Key:     k.v,
			Payload: p.v,
		}
	}
	return Value{
		t: Dict(keyT, payloadT),
		v: &Ydb.Value{
			Pairs: ps,
		},
	}
}

// It panics if vs is empty or contains not equal types.
func ListValue(n int, it func(int) V) Value {
	T := it(0).(Value).t
	var items = make([]*Ydb.Value, n)
	for i := 0; i < n; i++ {
		v := it(i).(Value)
		if !TypesEqual(v.t, T) {
			panic(fmt.Sprintf(
				"unexpected item type: %s; want %s",
				v.t, T,
			))
		}
		items[i] = v.v
	}
	return Value{
		t: ListType{T},
		v: &Ydb.Value{
			Items: items,
		},
	}
}

func VariantValue(x V, i uint32, t T) Value {
	v, ok := t.(VariantType)
	if !ok {
		panic(fmt.Sprintf("not a variant type: %s", t))
	}
	exp, ok := v.at(int(i))
	if !ok {
		panic(fmt.Sprintf("no %d-th variant for %s", i, t))
	}
	val := x.(Value)
	if !TypesEqual(exp, val.t) {
		panic(fmt.Sprintf(
			"unexpected type for %d-th variant: %s; want %s",
			i, val.t, exp,
		))
	}
	return Value{
		t: t,
		v: &Ydb.Value{
			Value: &Ydb.Value_NestedValue{
				NestedValue: val.v,
			},
			VariantIndex: i,
		},
	}
}

func ZeroValue(t T) Value {
	v := new(Ydb.Value)
	switch t := t.(type) {
	case PrimitiveType:
		switch t {
		case TypeBool:
			v.Value = new(Ydb.Value_BoolValue)

		case TypeInt8, TypeInt16, TypeInt32:
			v.Value = new(Ydb.Value_Int32Value)

		case
			TypeUint8, TypeUint16, TypeUint32,
			TypeDate, TypeDatetime:

			v.Value = new(Ydb.Value_Uint32Value)

		case
			TypeInt64,
			TypeInterval:

			v.Value = new(Ydb.Value_Int64Value)

		case
			TypeUint64,
			TypeTimestamp:

			v.Value = new(Ydb.Value_Uint64Value)

		case TypeFloat:
			v.Value = new(Ydb.Value_FloatValue)

		case TypeDouble:
			v.Value = new(Ydb.Value_DoubleValue)

		case
			TypeUTF8, TypeYSON, TypeJSON, TypeJSONDocument, TypeDyNumber,
			TypeTzDate, TypeTzDatetime, TypeTzTimestamp:

			v.Value = new(Ydb.Value_TextValue)

		case TypeString:
			v.Value = new(Ydb.Value_BytesValue)

		case TypeUUID:
			v.Value = new(Ydb.Value_Low_128)

		default:
			panic("uncovered primitive type")
		}

	case OptionalType, VoidType, NullType:
		v.Value = new(Ydb.Value_NullFlagValue)

	case ListType, TupleType, StructType, DictType:
		// Nothing to do.

	case DecimalType:
		v.Value = new(Ydb.Value_Low_128)

	case VariantType:
		panic("do not know what to do with variant type for zero value")

	default:
		panic("uncovered type")
	}
	return Value{
		t: t,
		v: v,
	}
}

// NullValue returns NULL value of given type T.
//
// For example, if T is Int32Type, then NullValue(Int32Type) will return value
// of type Optional<Int32Type> with NULL value.
//
// Nested optional types are handled also.
func NullValue(t T) Value {
	v := &Ydb.Value{
		Value: new(Ydb.Value_NullFlagValue),
	}
	x := t
	for {
		opt, ok := x.(OptionalType)
		if !ok {
			break
		}
		x = opt.T
		v = &Ydb.Value{
			Value: &Ydb.Value_NestedValue{
				NestedValue: v,
			},
		}
	}
	return Value{
		t: OptionalType{T: t},
		v: v,
	}
}

func OptionalValue(v V) Value {
	var (
		x   = v.(Value)
		typ = x.t
		val = x.v
	)
	_, opt := typ.(OptionalType)
	if opt {
		val = &Ydb.Value{
			Value: &Ydb.Value_NestedValue{
				NestedValue: val,
			},
		}
	}
	return Value{
		t: OptionalType{T: x.t},
		v: val,
	}
}
