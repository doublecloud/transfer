package util

import (
	"fmt"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestDeepSizeof(t *testing.T) {
	boolValue := true
	int64Value := int64(64)
	interfaceValue := interface{}(int64Value)
	uint64Value := uint64(64)
	stringValue := "0123456789"
	emptySlice := []int{}
	stringSlice := []string{"a", "b"}

	emptyStruct := struct{}{}
	allPublicFieldsStruct := struct {
		I  int64
		UI uint64
		S  string
	}{int64Value, uint64Value, stringValue}

	interfaceSlice := []interface{}{"a", "b", stringSlice, allPublicFieldsStruct}

	unexportedStringFieldStruct := struct {
		i  int64
		ui uint64
		s  string
	}{int64Value, uint64Value, stringValue}
	interfaceFieldStruct := struct {
		I  int64
		II interface{}
	}{int64Value, int64Value}

	interfaceMapValue := map[string]interface{}{
		"i": int64Value,
		"s": stringValue,
	}

	type testCase struct {
		name  string
		value interface{}
		size  uint64
	}

	cases := []testCase{
		{
			"bool",
			boolValue,
			uint64(unsafe.Sizeof(boolValue)),
		},
		{
			"int64",
			int64Value,
			uint64(unsafe.Sizeof(int64Value)),
		},
		{
			"interface",
			interfaceValue,
			uint64(unsafe.Sizeof(int64Value)),
		},
		{
			"uint64",
			uint64Value,
			uint64(unsafe.Sizeof(uint64Value)),
		},
		{
			"string",
			stringValue,
			uint64(unsafe.Sizeof(stringValue)) + uint64(len(stringValue)),
		},
		{
			"empty slice",
			emptySlice,
			uint64(unsafe.Sizeof(emptySlice)),
		},
		{
			"string slice",
			stringSlice,
			uint64(unsafe.Sizeof(stringSlice)) + // slice header
				2*uint64(unsafe.Sizeof("")) + // payload string references
				2, // bytes owned by strings
		},
		{
			"interface slice",
			interfaceSlice,
			uint64(unsafe.Sizeof(interfaceSlice)) + // slice header
				4*uint64(unsafe.Sizeof(interface{}(""))) + // empty interface struct - for each slice element
				// 0 and 1 elemetns
				2*uint64(unsafe.Sizeof("")) + // string references
				2 + // string bytes
				// 2 element: string slice
				uint64(unsafe.Sizeof(stringSlice)) + // slice header
				2*uint64(unsafe.Sizeof("")) + // payload string references
				2 + // bytes owned by strings
				// 3 element: struct with all public fields
				uint64(unsafe.Sizeof(int64Value)+
					unsafe.Sizeof(uint64Value)+
					unsafe.Sizeof(stringValue)) + uint64(len(stringValue)),
		},
		{
			"empty struct",
			emptyStruct,
			uint64(unsafe.Sizeof(emptyStruct)),
		},
		{
			"struct with all public fields",
			allPublicFieldsStruct,
			uint64(unsafe.Sizeof(int64Value)+
				unsafe.Sizeof(uint64Value)+
				unsafe.Sizeof(stringValue)) + uint64(len(stringValue)),
		},
		{
			"struct with unexported fields",
			unexportedStringFieldStruct,
			uint64(unsafe.Sizeof(int64Value) +
				unsafe.Sizeof(uint64Value) +
				unsafe.Sizeof(stringValue)), // only string reference for unexported field
		},
		{
			"struct with interface field",
			interfaceFieldStruct,
			uint64(2*unsafe.Sizeof(int64Value) + // I and II fields values
				unsafe.Sizeof(interface{}(int64Value))), // II empty struct
		},
		{
			"map with interface value",
			interfaceMapValue,
			uint64(unsafe.Sizeof(interfaceMapValue)+ // map header
				2*unsafe.Sizeof("")+2+ // string keys size
				2*unsafe.Sizeof(interface{}(""))+ // empty interface structs size for values
				unsafe.Sizeof(int64Value)+unsafe.Sizeof(stringValue)) + uint64(len(stringValue)), // values payload
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("test case/%v:%v", i, c.name), func(t *testing.T) {
			require.Equal(t, DeepSizeof(c.value), c.size)
		})
	}
}
