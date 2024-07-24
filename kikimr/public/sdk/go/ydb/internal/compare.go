package internal

import (
	"bytes"
	"fmt"
	"math/big"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

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
func Compare(l, r V) (int, error) {
	return compare(unwrapTypedValue(l.toYDB()), unwrapTypedValue(r.toYDB()))
}

func unwrapTypedValue(v *Ydb.TypedValue) *Ydb.TypedValue {
	typ := v.Type
	val := v.Value
	for opt := typ.GetOptionalType(); opt != nil; opt = typ.GetOptionalType() {
		typ = opt.Item
		if nested := val.GetNestedValue(); nested != nil {
			val = nested
		}
	}
	return &Ydb.TypedValue{Type: typ, Value: val}
}

func compare(l *Ydb.TypedValue, r *Ydb.TypedValue) (int, error) {
	lTypeID := l.Type.GetTypeId()
	rTypeID := r.Type.GetTypeId()
	switch {
	case lTypeID != rTypeID:
		return 0, notComparableError(l, r)
	case lTypeID != Ydb.Type_PRIMITIVE_TYPE_ID_UNSPECIFIED:
		return comparePrimitives(lTypeID, l.Value, r.Value)
	case l.Type.GetTupleType() != nil && r.Type.GetTupleType() != nil:
		return compareTuplesOrLists(expandTuple(l), expandTuple(r))
	case l.Type.GetListType() != nil && r.Type.GetListType() != nil:
		return compareTuplesOrLists(expandList(l), expandList(r))
	default:
		return 0, notComparableError(l, r)
	}
}

func expandList(v *Ydb.TypedValue) []*Ydb.TypedValue {
	elementType := v.Type.GetListType().Item
	size := len(v.Value.Items)
	values := make([]*Ydb.TypedValue, 0, size)
	for _, val := range v.Value.Items {
		values = append(values, unwrapTypedValue(&Ydb.TypedValue{Type: elementType, Value: val}))
	}
	return values
}

func expandTuple(v *Ydb.TypedValue) []*Ydb.TypedValue {
	tuple := v.Type.GetTupleType()
	size := len(tuple.Elements)
	values := make([]*Ydb.TypedValue, 0, size)
	for idx, typ := range tuple.Elements {
		values = append(values, unwrapTypedValue(&Ydb.TypedValue{Type: typ, Value: v.Value.Items[idx]}))
	}
	return values
}

func notComparableError(l interface{}, r interface{}) error {
	return fmt.Errorf("not comparable: %v and %v", l, r)
}

func comparePrimitives(t Ydb.Type_PrimitiveTypeId, l *Ydb.Value, r *Ydb.Value) (int, error) {
	_, lIsNull := l.Value.(*Ydb.Value_NullFlagValue)
	_, rIsNull := r.Value.(*Ydb.Value_NullFlagValue)
	if lIsNull {
		if rIsNull {
			return 0, nil
		}
		return -1, nil
	}
	if rIsNull {
		return 1, nil
	}

	if compare, found := comparators[t]; found {
		return compare(l, r), nil
	}
	// special cases
	switch t {
	case Ydb.Type_DYNUMBER:
		return compareDyNumber(l, r)
	default:
		return 0, notComparableError(l, r)
	}
}

func compareTuplesOrLists(l []*Ydb.TypedValue, r []*Ydb.TypedValue) (int, error) {
	for i, lval := range l {
		if i >= len(r) {
			// l is longer than r, first len(r) elements equal
			return 1, nil
		}
		rval := r[i]
		cmp, err := compare(lval, rval)
		if err != nil {
			return 0, err
		}
		if cmp != 0 {
			return cmp, nil
		}
	}
	// len(l) elements equal
	if len(r) > len(l) {
		return -1, nil
	}
	return 0, nil
}

type comparator func(l, r *Ydb.Value) int

var comparators = map[Ydb.Type_PrimitiveTypeId]comparator{
	Ydb.Type_BOOL:      compareBool,
	Ydb.Type_INT8:      compareInt32,
	Ydb.Type_UINT8:     compareUint32,
	Ydb.Type_INT16:     compareInt32,
	Ydb.Type_UINT16:    compareUint32,
	Ydb.Type_INT32:     compareInt32,
	Ydb.Type_UINT32:    compareUint32,
	Ydb.Type_INT64:     compareInt64,
	Ydb.Type_UINT64:    compareUint64,
	Ydb.Type_FLOAT:     compareFloat,
	Ydb.Type_DOUBLE:    compareDouble,
	Ydb.Type_DATE:      compareUint32,
	Ydb.Type_DATETIME:  compareUint32,
	Ydb.Type_TIMESTAMP: compareUint64,
	Ydb.Type_INTERVAL:  compareInt64,
	Ydb.Type_STRING:    compareBytes,
	Ydb.Type_UTF8:      compareText,
	Ydb.Type_UUID:      compareUUID,
}

func compareUint32(l, r *Ydb.Value) int {
	ll := l.GetUint32Value()
	rr := r.GetUint32Value()
	switch {
	case ll < rr:
		return -1
	case ll > rr:
		return 1
	default:
		return 0
	}
}

func compareInt32(l, r *Ydb.Value) int {
	ll := l.GetInt32Value()
	rr := r.GetInt32Value()
	switch {
	case ll < rr:
		return -1
	case ll > rr:
		return 1
	default:
		return 0
	}
}

func compareUint64(l, r *Ydb.Value) int {
	ll := l.GetUint64Value()
	rr := r.GetUint64Value()
	switch {
	case ll < rr:
		return -1
	case ll > rr:
		return 1
	default:
		return 0
	}
}

func compareInt64(l, r *Ydb.Value) int {
	ll := l.GetInt64Value()
	rr := r.GetInt64Value()
	switch {
	case ll < rr:
		return -1
	case ll > rr:
		return 1
	default:
		return 0
	}
}

func compareFloat(l, r *Ydb.Value) int {
	ll := l.GetFloatValue()
	rr := r.GetFloatValue()
	switch {
	case ll < rr:
		return -1
	case ll > rr:
		return 1
	default:
		return 0
	}
}

func compareDouble(l, r *Ydb.Value) int {
	ll := l.GetDoubleValue()
	rr := r.GetDoubleValue()
	switch {
	case ll < rr:
		return -1
	case ll > rr:
		return 1
	default:
		return 0
	}
}

func compareText(l, r *Ydb.Value) int {
	ll := l.GetTextValue()
	rr := r.GetTextValue()
	return strings.Compare(ll, rr)
}

func compareBytes(l, r *Ydb.Value) int {
	ll := l.GetBytesValue()
	rr := r.GetBytesValue()
	return bytes.Compare(ll, rr)
}

func compareBool(l, r *Ydb.Value) int {
	ll := l.GetBoolValue()
	rr := r.GetBoolValue()
	if ll {
		if rr {
			return 0
		} else {
			return 1
		}
	}
	if rr {
		return -1
	}
	return 0
}

func compareDyNumber(l, r *Ydb.Value) (int, error) {
	ll := l.GetTextValue()
	rr := r.GetTextValue()
	lf, _, err := big.ParseFloat(ll, 10, 127, big.ToNearestEven)
	if err != nil {
		return 0, err
	}
	rf, _, err := big.ParseFloat(rr, 10, 127, big.ToNearestEven)
	if err != nil {
		return 0, err
	}
	return lf.Cmp(rf), nil
}

func compareUUID(l, r *Ydb.Value) int {
	lh := l.GetHigh_128()
	rh := r.GetHigh_128()
	switch {
	case lh > rh:
		return 1
	case lh < rh:
		return -1
	}
	ll := l.GetLow_128()
	rl := r.GetLow_128()
	switch {
	case ll < rl:
		return -1
	case ll > rl:
		return 1
	default:
		return 0
	}

}
