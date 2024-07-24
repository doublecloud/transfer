package internal

import (
	"testing"

	structpb "github.com/golang/protobuf/ptypes/struct"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

func TestUnwrapOptionalValue(t *testing.T) {
	v := OptionalValue(OptionalValue(UTF8Value("a")))
	val := unwrapTypedValue(v.toYDB())
	typeID := val.Type.GetTypeId()
	if typeID != Ydb.Type_UTF8 {
		t.Errorf("Types are different: expected %d, actual %d", Ydb.Type_UTF8, typeID)
	}
	textValue := val.Value.Value.(*Ydb.Value_TextValue)
	text := textValue.TextValue
	if text != "a" {
		t.Errorf("Values are different: expected %q, actual %q", "a", text)
	}
}

func TestUnwrapPrimitiveValue(t *testing.T) {
	v := UTF8Value("a")
	val := unwrapTypedValue(v.toYDB())
	typeID := val.Type.GetTypeId()
	if typeID != Ydb.Type_UTF8 {
		t.Errorf("Types are different: expected %d, actual %d", Ydb.Type_UTF8, typeID)
	}
	textValue := val.Value.Value.(*Ydb.Value_TextValue)
	text := textValue.TextValue
	if text != "a" {
		t.Errorf("Values are different: expected %q, actual %q", "a", text)
	}
}

func TestUnwrapNullValue(t *testing.T) {
	v := NullValue(TypeUTF8)
	val := unwrapTypedValue(v.toYDB())
	typeID := val.Type.GetTypeId()
	if typeID != Ydb.Type_UTF8 {
		t.Errorf("Types are different: expected %d, actual %d", Ydb.Type_UTF8, typeID)
	}
	nullFlagValue := val.Value.Value.(*Ydb.Value_NullFlagValue)
	if nullFlagValue.NullFlagValue != structpb.NullValue_NULL_VALUE {
		t.Errorf("Values are different: expected %d, actual %d", structpb.NullValue_NULL_VALUE, nullFlagValue.NullFlagValue)
	}
}
