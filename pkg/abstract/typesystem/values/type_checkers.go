package values

import (
	"encoding/json"
	"reflect"
	"time"

	"go.ytsaurus.tech/yt/go/schema"
)

// ValueTypeChecker is a function which returns true if the passed value is represented properly - that is, in accordance with the requirements of the YT type this checker represents.
type ValueTypeChecker func(interface{}) bool

func oneOf(canonTypes ...interface{}) ValueTypeChecker {
	return func(val interface{}) bool {
		for _, canon := range canonTypes {
			if reflect.TypeOf(val) == reflect.TypeOf(canon) {
				return true
			}
		}
		return false
	}
}

func strict(canon interface{}) ValueTypeChecker {
	return func(val interface{}) bool {
		return reflect.TypeOf(val) == reflect.TypeOf(canon)
	}
}

func jsonSerializeable(val interface{}) bool {
	_, err := json.Marshal(val)
	return err == nil
}

// OneofValueTypeCheckers returns checkers permitting different representations for a particular YT type
//
// TODO: TM-4130: switch to StrictValueTypeCheckers().
func OneofValueTypeCheckers() map[schema.Type]ValueTypeChecker {
	return map[schema.Type]ValueTypeChecker{
		schema.TypeBoolean:   strict(false),
		schema.TypeInt8:      oneOf(int8(0), int64(0)),
		schema.TypeInt16:     oneOf(int16(0), int64(0)),
		schema.TypeInt32:     oneOf(int(0), int32(0), int64(0)),
		schema.TypeInt64:     strict(int64(0)),
		schema.TypeUint8:     oneOf(uint8(0), uint64(0)),
		schema.TypeUint16:    oneOf(uint16(0), uint64(0)),
		schema.TypeUint32:    oneOf(uint32(0), uint64(0)),
		schema.TypeUint64:    oneOf(uint32(0), uint64(0)),
		schema.TypeFloat32:   oneOf(float32(0), float64(0)),
		schema.TypeFloat64:   oneOf(float32(0), float64(0), json.Number("0")),
		schema.TypeBytes:     oneOf(string(""), []byte("")),
		schema.TypeString:    oneOf(string(""), []byte("")),
		schema.TypeDate:      strict(time.Time{}),
		schema.TypeDatetime:  strict(time.Time{}),
		schema.TypeTimestamp: strict(time.Time{}),
		schema.TypeInterval:  strict(time.Duration(0)),
		schema.TypeAny:       jsonSerializeable,
	}
}

// StrictValueTypeCheckers returns checkers permitting only the single correct representation for a particular YT type.
func StrictValueTypeCheckers() map[schema.Type]ValueTypeChecker {
	return map[schema.Type]ValueTypeChecker{
		schema.TypeBoolean:   strict(false),
		schema.TypeInt8:      strict(int8(0)),
		schema.TypeInt16:     strict(int16(0)),
		schema.TypeInt32:     strict(int32(0)),
		schema.TypeInt64:     strict(int64(0)),
		schema.TypeUint8:     strict(uint8(0)),
		schema.TypeUint16:    strict(uint16(0)),
		schema.TypeUint32:    strict(uint32(0)),
		schema.TypeUint64:    strict(uint64(0)),
		schema.TypeFloat32:   strict(float32(0)),
		schema.TypeFloat64:   strict(json.Number("0")),
		schema.TypeBytes:     strict([]byte("")),
		schema.TypeString:    strict(string("")),
		schema.TypeDate:      strict(time.Time{}),
		schema.TypeDatetime:  strict(time.Time{}),
		schema.TypeTimestamp: strict(time.Time{}),
		schema.TypeInterval:  strict(time.Duration(0)),
		schema.TypeAny:       jsonSerializeable,
	}
}
