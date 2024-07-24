package ydb

import (
	"strings"
	"testing"
)

func TestUint8(t *testing.T) {
	l := Uint8Value(byte(1))
	r := Uint8Value(byte(10))
	c, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestInt8(t *testing.T) {
	l := Int8Value(int8(1))
	r := Int8Value(int8(10))
	c, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestTimestamp(t *testing.T) {
	l := TimestampValue(1)
	r := TimestampValue(10)
	c, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestDateTime(t *testing.T) {
	l := DatetimeValue(1)
	r := DatetimeValue(10)
	c, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestUint64(t *testing.T) {
	l := Uint64Value(uint64(1))
	r := Uint64Value(uint64(10))
	c, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestInt64(t *testing.T) {
	l := Int64Value(int64(1))
	r := Int64Value(int64(10))
	c, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestDouble(t *testing.T) {
	l := DoubleValue(1.0)
	r := DoubleValue(2.0)
	c, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestFloat(t *testing.T) {
	l := FloatValue(1.0)
	r := FloatValue(2.0)
	c, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestUTF8(t *testing.T) {
	l := UTF8Value("abc")
	r := UTF8Value("abx")
	c, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestOptionalUTF8(t *testing.T) {
	l := OptionalValue(OptionalValue(UTF8Value("abc")))
	r := UTF8Value("abx")
	c, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestString(t *testing.T) {
	l := StringValue([]byte{1, 2, 3})
	r := StringValue([]byte{1, 2, 5})
	c, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestNull(t *testing.T) {
	l := NullValue(TypeUTF8)
	r := UTF8Value("abc")
	c, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestTuple(t *testing.T) {
	withNull := TupleValue(Uint64Value(1), NullValue(TypeUTF8))
	least := TupleValue(Uint64Value(1), UTF8Value("abc"))
	medium := TupleValue(Uint64Value(1), UTF8Value("def"))
	largest := TupleValue(Uint64Value(2), UTF8Value("abc"))

	c, err := Compare(least, medium)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(least, largest)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(largest, medium)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = Compare(withNull, least)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(withNull, withNull)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)

}

func TestList(t *testing.T) {
	least := ListValue(Uint64Value(1), Uint64Value(1))
	medium := ListValue(Uint64Value(1), Uint64Value(2))
	largest := ListValue(Uint64Value(2), Uint64Value(1))

	c, err := Compare(least, medium)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(least, largest)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(largest, medium)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

}

func TestDyNumber(t *testing.T) {
	l := DyNumberValue("2")
	r := DyNumberValue("12")
	c, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestUUID(t *testing.T) {
	l := UUIDValue([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	r := UUIDValue([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17})
	g := UUIDValue([16]byte{100, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17})
	c, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(l, g)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestIncompatiblePrimitives(t *testing.T) {
	l := Uint64Value(1)
	r := TimestampValue(2)
	_, err := Compare(l, r)
	if err == nil {
		t.Errorf("Error expected")
	}
	if !strings.HasPrefix(err.Error(), "not comparable:") {
		t.Errorf("Unexpected error message: %s", err.Error())
	}
}

func TestIncompatibleTuples(t *testing.T) {
	l := TupleValue(Uint64Value(1), UTF8Value("abc"))
	r := TupleValue(Uint64Value(1), StringValue([]byte("abc")))
	_, err := Compare(l, r)
	if err == nil {
		t.Error("Error expected")
	} else {
		if !strings.HasPrefix(err.Error(), "not comparable:") {
			t.Errorf("Unexpected error message: %s", err.Error())
		}
	}
}

func TestTupleOfDifferentLength(t *testing.T) {
	l := TupleValue(Uint64Value(1), UTF8Value("abc"))
	r := TupleValue(Uint64Value(1), UTF8Value("abc"), UTF8Value("def"))

	cmp, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, cmp)

	cmp, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, cmp)
}

func TestTupleInTuple(t *testing.T) {
	l := TupleValue(Uint64Value(1), TupleValue(UTF8Value("abc"), StringValue([]byte("xyz"))))
	r := TupleValue(Uint64Value(1), TupleValue(UTF8Value("def"), StringValue([]byte("xyz"))))

	cmp, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, cmp)

	cmp, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, cmp)

	cmp, err = Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, cmp)
}

func TestListInList(t *testing.T) {
	l := ListValue(ListValue(UTF8Value("abc"), UTF8Value("def")), ListValue(UTF8Value("uvw"), UTF8Value("xyz")))
	r := ListValue(ListValue(UTF8Value("abc"), UTF8Value("deg")), ListValue(UTF8Value("uvw"), UTF8Value("xyz")))

	cmp, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, cmp)

	cmp, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, cmp)

	cmp, err = Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, cmp)
}

func requireNoError(t *testing.T, err error) {
	if err != nil {
		t.Errorf("Unexpected error %v", err)
	}
}

func requireEqualValues(t *testing.T, expected int, actual int) {
	if expected != actual {
		t.Errorf("Values not equal: expected %v, actual %v", expected, actual)
	}
}
