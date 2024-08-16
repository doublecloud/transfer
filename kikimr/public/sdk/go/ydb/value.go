package ydb

import (
	"time"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/internal"
)

type Value interface {
	internal.V
}

func BoolValue(v bool) Value        { return internal.BoolValue(v) }
func Int8Value(v int8) Value        { return internal.Int8Value(v) }
func Uint8Value(v uint8) Value      { return internal.Uint8Value(v) }
func Int16Value(v int16) Value      { return internal.Int16Value(v) }
func Uint16Value(v uint16) Value    { return internal.Uint16Value(v) }
func Int32Value(v int32) Value      { return internal.Int32Value(v) }
func Uint32Value(v uint32) Value    { return internal.Uint32Value(v) }
func Int64Value(v int64) Value      { return internal.Int64Value(v) }
func Uint64Value(v uint64) Value    { return internal.Uint64Value(v) }
func FloatValue(v float32) Value    { return internal.FloatValue(v) }
func DoubleValue(v float64) Value   { return internal.DoubleValue(v) }
func DateValue(v uint32) Value      { return internal.DateValue(v) }
func DatetimeValue(v uint32) Value  { return internal.DatetimeValue(v) }
func TimestampValue(v uint64) Value { return internal.TimestampValue(v) }

// IntervalValue makes Value from microseconds
//
// Deprecated: use IntervalValueFromMicroseconds instead
func IntervalValue(v int64) Value { return internal.IntervalValue(v) }

// IntervalValueFromMicroseconds makes Value from microseconds
func IntervalValueFromMicroseconds(v int64) Value { return internal.IntervalValue(v) }

func TzDateValue(v string) Value          { return internal.TzDateValue(v) }
func TzDatetimeValue(v string) Value      { return internal.TzDatetimeValue(v) }
func TzTimestampValue(v string) Value     { return internal.TzTimestampValue(v) }
func DateValueFromTime(v time.Time) Value { return internal.DateValue(internal.MarshalDate(v)) }
func DatetimeValueFromTime(v time.Time) Value {
	return internal.DatetimeValue(internal.MarshalDatetime(v))
}
func TimestampValueFromTime(v time.Time) Value {
	return internal.TimestampValue(internal.MarshalTimestamp(v))
}
func IntervalValueFromDuration(v time.Duration) Value {
	return internal.IntervalValue(internal.DurationToMicroseconds(v))
}
func TzDateValueFromTime(v time.Time) Value { return internal.TzDateValue(internal.MarshalTzDate(v)) }
func TzDatetimeValueFromTime(v time.Time) Value {
	return internal.TzDatetimeValue(internal.MarshalTzDatetime(v))
}
func TzTimestampValueFromTime(v time.Time) Value {
	return internal.TzTimestampValue(internal.MarshalTzTimestamp(v))
}
func StringValue(v []byte) Value                { return internal.StringValue(v) }
func StringValueFromString(v string) Value      { return internal.StringValue([]byte(v)) }
func UTF8Value(v string) Value                  { return internal.UTF8Value(v) }
func YSONValue(v string) Value                  { return internal.YSONValue([]byte(v)) }
func YSONValueFromBytes(v []byte) Value         { return internal.YSONValue(v) }
func JSONValue(v string) Value                  { return internal.JSONValue(v) }
func JSONValueFromBytes(v []byte) Value         { return internal.JSONValue(string(v)) }
func UUIDValue(v [16]byte) Value                { return internal.UUIDValue(v) }
func JSONDocumentValue(v string) Value          { return internal.JSONDocumentValue(v) }
func JSONDocumentValueFromBytes(v []byte) Value { return internal.JSONDocumentValue(string(v)) }
func DyNumberValue(v string) Value              { return internal.DyNumberValue(v) }

func VoidValue() Value            { return internal.VoidValue }
func NullValue(t Type) Value      { return internal.NullValue(t) }
func ZeroValue(t Type) Value      { return internal.ZeroValue(t) }
func OptionalValue(v Value) Value { return internal.OptionalValue(v) }

// DecimalValue creates decimal value of given type t and value v.
// Note that v interpreted as big-endian int128.
func DecimalValue(t Type, v [16]byte) Value {
	return internal.DecimalValue(t, v)
}

func TupleValue(vs ...Value) Value {
	return internal.TupleValue(len(vs), func(i int) internal.V {
		return vs[i]
	})
}

func ListValue(vs ...Value) Value {
	return internal.ListValue(len(vs), func(i int) internal.V {
		return vs[i]
	})
}

type tStructValueProto internal.StructValueProto

type StructValueOption func(*tStructValueProto)

func StructFieldValue(name string, value Value) StructValueOption {
	return func(p *tStructValueProto) {
		(*internal.StructValueProto)(p).Add(name, value)
	}
}

func StructValue(opts ...StructValueOption) Value {
	var p tStructValueProto
	for _, opt := range opts {
		opt(&p)
	}
	return internal.StructValue((*internal.StructValueProto)(&p))
}

func DictValue(pairs ...Value) Value {
	return internal.DictValue(len(pairs), func(i int) internal.V {
		return pairs[i]
	})
}

func VariantValue(v Value, i uint32, variantT Type) Value {
	return internal.VariantValue(v, i, variantT)
}

// Compare compares its operands.
// It returns -1, 0, 1 if l < r, l == r, l > r. Returns error if types are not comparable.
// Comparable types are all integer types, UUID, DyNumber, Float, Double, String, UTF8,
// Date, Datetime, Timestamp, Tuples and Lists.
// Primitive arguments are comparable if their types are the same.
// Optional type is comparable to underlying type, e.g. Optional<Optional<Float>> is comparable to Float.
// Null value is comparable to non-null value of the same type and is considered less than any non-null value.
// Tuples and Lists are comparable if their elements are comparable.
// Tuples and Lists are compared lexicographically. If tuples (lists) have different length and elements of the
// shorter tuple (list) are all equal to corresponding elements of the other tuple (list), than the shorter tuple (list)
// is considered less than the longer one.
func Compare(l, r Value) (int, error) {
	return internal.Compare(l, r)
}
