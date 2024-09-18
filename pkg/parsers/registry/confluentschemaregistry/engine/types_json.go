package engine

import (
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

type jsonType string

const (
	JSONTypeArray   jsonType = "array"
	JSONTypeBoolean jsonType = "boolean"
	JSONTypeInteger jsonType = "integer"
	JSONTypeNumber  jsonType = "number"
	JSONTypeNull    jsonType = "null"
	JSONTypeObject  jsonType = "object"
	JSONTypeString  jsonType = "string"
)

func (j jsonType) String() string {
	return string(j)
}

var jsonSchemaTypes = map[jsonType]ytschema.Type{
	JSONTypeArray:   ytschema.TypeAny,
	JSONTypeBoolean: ytschema.TypeBoolean,
	JSONTypeInteger: ytschema.TypeInt64,
	JSONTypeNumber:  ytschema.TypeFloat64,
	JSONTypeObject:  ytschema.TypeAny,
	JSONTypeString:  ytschema.TypeString,
}
