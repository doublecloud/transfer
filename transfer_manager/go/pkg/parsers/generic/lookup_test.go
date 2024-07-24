package generic

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLookupComplexMap(t *testing.T) {
	expected := make(map[string]interface{})
	obj := map[string]interface{}{"f1": map[string]interface{}{"f2": expected}}
	result, err := lookupComplex(obj, "f1.f2")
	require.NoError(t, err)
	require.Equal(t, expected, result)
}

func TestLookupComplexJson(t *testing.T) {
	expected := "expected"
	json, err := json.Marshal(map[string]interface{}{"f2": expected})
	require.NoError(t, err)
	obj := map[string]interface{}{"f1": string(json)}
	result, err := lookupComplex(obj, "f1.f2")
	require.NoError(t, err)
	require.Equal(t, expected, result)
}

func TestLookupComplexInvalidJson(t *testing.T) {
	obj := map[string]interface{}{"f1": "invalid json"}
	result, err := lookupComplex(obj, "f1.f2")
	require.Nil(t, result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unable to parse json")
}

func TestLookupComplexUnexpectedValueType(t *testing.T) {
	obj := map[string]interface{}{"f1": 123}
	result, err := lookupComplex(obj, "f1.f2")
	require.Nil(t, result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected value type")
}

func TestLookupComplexInvalidPath(t *testing.T) {
	obj := map[string]interface{}{"f1": "value"}
	result, err := lookupComplex(obj, "f2")
	require.Nil(t, result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unable to get field")
}
