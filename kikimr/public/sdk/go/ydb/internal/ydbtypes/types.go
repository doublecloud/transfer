/*
Package ydbtypes provides tools for integration ydb types with go/types package.
It is intended primarily for writing generation tools and linters.
*/package ydbtypes

import (
	"fmt"
	"go/types"
	"strings"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/internal"
)

func PrimitiveTypeFromString(s string) (t internal.PrimitiveType, err error) {
	switch strings.ToLower(s) {
	case "bool":
		t = internal.TypeBool
	case "int8":
		t = internal.TypeInt8
	case "uint8":
		t = internal.TypeUint8
	case "int16":
		t = internal.TypeInt16
	case "uint16":
		t = internal.TypeUint16
	case "int32":
		t = internal.TypeInt32
	case "uint32":
		t = internal.TypeUint32
	case "int64":
		t = internal.TypeInt64
	case "uint64":
		t = internal.TypeUint64
	case "float":
		t = internal.TypeFloat
	case "double":
		t = internal.TypeDouble
	case "date":
		t = internal.TypeDate
	case "datetime":
		t = internal.TypeDatetime
	case "timestamp":
		t = internal.TypeTimestamp
	case "interval":
		t = internal.TypeInterval
	case "tzdate":
		t = internal.TypeTzDate
	case "tzdatetime":
		t = internal.TypeTzDatetime
	case "tztimestamp":
		t = internal.TypeTzTimestamp
	case "string":
		t = internal.TypeString
	case "utf8":
		t = internal.TypeUTF8
	case "yson":
		t = internal.TypeYSON
	case "json":
		t = internal.TypeJSON
	case "uuid":
		t = internal.TypeUUID
	case "jsondocument":
		t = internal.TypeJSONDocument
	case "dynumber":
		t = internal.TypeDyNumber
	default:
		err = fmt.Errorf("ydbtypes: unknown type: %q", s)
	}
	return
}

func PrimitiveTypeFromGoType(typ types.Type) (t internal.PrimitiveType, err error) {
	switch t := typ.(type) {
	case *types.Slice:
		b, basic := t.Elem().(*types.Basic)
		if basic && b.Kind() == types.Byte {
			return internal.TypeString, nil
		}

	case *types.Basic:
		switch t.Kind() {
		case types.Bool:
			return internal.TypeBool, nil
		case types.Int8:
			return internal.TypeInt8, nil
		case types.Int16:
			return internal.TypeInt16, nil
		case types.Int32:
			return internal.TypeInt32, nil
		case types.Int64:
			return internal.TypeInt64, nil
		case types.Uint8:
			return internal.TypeUint8, nil
		case types.Uint16:
			return internal.TypeUint16, nil
		case types.Uint32:
			return internal.TypeUint32, nil
		case types.Uint64:
			return internal.TypeUint64, nil
		case types.Float32:
			return internal.TypeFloat, nil
		case types.Float64:
			return internal.TypeDouble, nil
		case types.String:
			return internal.TypeUTF8, nil

		case types.Int, types.Uint:
			return 0, fmt.Errorf(
				"ydbtypes: ambiguous mapping for go %s type exists",
				typ,
			)
		}
	}
	return 0, fmt.Errorf("ydbtypes: unsupported type: %s", typ)
}

var (
	bytesArray16 = types.NewArray(types.Typ[types.Byte], 16)
	bytesSlice   = types.NewSlice(types.Typ[types.Byte])
)

func GoTypeFromPrimitiveType(t internal.PrimitiveType) types.Type {
	switch t {
	case internal.TypeBool:
		return types.Typ[types.Bool]

	case internal.TypeInt8:
		return types.Typ[types.Int8]

	case internal.TypeUint8:
		return types.Typ[types.Uint8]

	case internal.TypeInt16:
		return types.Typ[types.Int16]

	case internal.TypeUint16:
		return types.Typ[types.Uint16]

	case internal.TypeInt32:
		return types.Typ[types.Int32]

	case internal.TypeUint32:
		return types.Typ[types.Uint32]

	case internal.TypeInt64:
		return types.Typ[types.Int64]

	case internal.TypeUint64:
		return types.Typ[types.Uint64]

	case internal.TypeFloat:
		return types.Typ[types.Float32]

	case internal.TypeDouble:
		return types.Typ[types.Float64]

	case internal.TypeDate:
		return types.Typ[types.Uint32]

	case internal.TypeDatetime:
		return types.Typ[types.Uint32]

	case internal.TypeTimestamp:
		return types.Typ[types.Uint64]

	case internal.TypeInterval:
		return types.Typ[types.Int64]

	case internal.TypeTzDate:
		return types.Typ[types.String]

	case internal.TypeTzDatetime:
		return types.Typ[types.String]

	case internal.TypeTzTimestamp:
		return types.Typ[types.String]

	case internal.TypeUTF8:
		return types.Typ[types.String]

	case internal.TypeYSON:
		return types.Typ[types.String]

	case internal.TypeJSON:
		return types.Typ[types.String]

	case internal.TypeUUID:
		return bytesArray16

	case internal.TypeJSONDocument:
		return types.Typ[types.String]

	case internal.TypeDyNumber:
		return types.Typ[types.String]

	case internal.TypeString:
		return bytesSlice

	default:
		panic("uncovered primitive type")
	}
}
