package internal

import (
	"bytes"
	"testing"
)

func TestValueToString(t *testing.T) {
	for _, test := range []struct {
		value V
		exp   string
	}{
		{
			value: VoidValue,
		},
		{
			value: Int32Value(42),
		},
		{
			value: NullValue(TypeInt32),
		},
		{
			value: NullValue(OptionalType{T: TypeBool}),
		},
		{
			value: OptionalValue(OptionalValue(Int32Value(42))),
		},
		{
			value: OptionalValue(OptionalValue(OptionalValue(Int32Value(42)))),
		},
		{
			value: ListValue(4, func(i int) V {
				return Int32Value(int32(i))
			}),
		},
		{
			value: TupleValue(4, func(i int) V {
				return Int32Value(int32(i))
			}),
		},
		{
			value: VariantValue(Int32Value(42), 1, VariantType{T: TupleType{
				Elems: []T{
					TypeString,
					TypeInt32,
				},
			}}),
		},
		{
			value: VariantValue(Int32Value(42), 1, VariantType{S: StructType{
				Fields: []StructField{
					{"foo", TypeString},
					{"bar", TypeInt32},
				},
			}}),
		},
		{
			value: DictValue(4, func(i int) V {
				switch i {
				// Key items.
				case 0:
					return UTF8Value("foo")
				case 2:
					return UTF8Value("bar")
				}
				// Value items.
				switch i {
				case 1:
					return Int32Value(42)
				case 3:
					return Int32Value(43)
				}
				panic("whoa")
			}),
		},
		{
			value: DictValue(4, func(i int) V {
				switch i {
				// Key items.
				case 0:
					return UTF8Value("foo")
				case 2:
					return UTF8Value("bar")
				}
				// Value items.
				if i%2 != 0 {
					return VoidValue
				}
				panic("whoa")
			}),
		},
		{
			value: ZeroValue(OptionalType{T: TypeBool}),
		},
		{
			value: ZeroValue(ListType{T: TypeBool}),
		},
		{
			value: ZeroValue(TypeUUID),
		},
	} {
		t.Run("", func(t *testing.T) {
			var buf bytes.Buffer
			test.value.toString(&buf)
			t.Logf("STRING: %q", buf.String())
		})
	}
}
