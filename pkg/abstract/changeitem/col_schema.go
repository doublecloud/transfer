package changeitem

import (
	"fmt"
	"strings"

	"go.ytsaurus.tech/yt/go/schema"
)

const (
	DefaultPropertyKey = "default"
)

type ColSchema struct {
	TableSchema  string `json:"table_schema"` // table namespace - for SQL it's database name
	TableName    string `json:"table_name"`
	Path         string `json:"path"`
	ColumnName   string `json:"name"`
	DataType     string `json:"type"` // string with YT type from arcadia/yt/go/schema/schema.go
	PrimaryKey   bool   `json:"key"`
	FakeKey      bool   `json:"fake_key"`      // TODO: remove after migration (TM-1225)
	Required     bool   `json:"required"`      // Required - it's about 'can field contains nil'
	Expression   string `json:"expression"`    // expression for generated columns
	OriginalType string `json:"original_type"` // db-prefix:db-specific-type. example: "mysql:bigint(20) unsigned"

	// field to carry additional optional info.
	// It's either nil or pair key-value. Value can be nil, if it's meaningful
	Properties map[PropertyKey]interface{} `json:"properties,omitempty"`
}

func NewColSchema(columnName string, dataType schema.Type, isPrimaryKey bool) ColSchema {
	return ColSchema{
		TableSchema:  "",
		TableName:    "",
		Path:         "",
		ColumnName:   columnName,
		DataType:     dataType.String(),
		PrimaryKey:   isPrimaryKey,
		FakeKey:      false,
		Required:     false,
		Expression:   "",
		OriginalType: "",
		Properties:   nil,
	}
}

func MakeOriginallyTypedColSchema(colName, dataType string, origType string) ColSchema {
	var colSchema ColSchema
	colSchema.ColumnName = colName
	colSchema.DataType = dataType
	colSchema.OriginalType = origType
	return colSchema
}

func MakeTypedColSchema(colName, dataType string, primaryKey bool) ColSchema {
	var colSchema ColSchema
	colSchema.ColumnName = colName
	colSchema.DataType = dataType
	colSchema.PrimaryKey = primaryKey
	return colSchema
}

func (c *ColSchema) AddProperty(key PropertyKey, val interface{}) {
	if c.Properties == nil {
		c.Properties = map[PropertyKey]interface{}{}
	}
	c.Properties[key] = val
}

func (c *ColSchema) Copy() *ColSchema {
	result := *c
	result.Properties = make(map[PropertyKey]interface{})
	for p, v := range c.Properties {
		result.Properties[p] = v
	}
	return &result
}

func (c *ColSchema) ColPath() string {
	if len(c.Path) > 0 {
		return c.Path
	}

	return c.ColumnName
}

func (c *ColSchema) Fqtn() string {
	return c.TableSchema + "_" + c.TableName
}

func (c *ColSchema) TableID() TableID {
	return TableID{Namespace: c.TableSchema, Name: c.TableName}
}

func (c *ColSchema) IsNestedKey() bool {
	return strings.ContainsAny(c.Path, ".") || strings.ContainsAny(c.Path, "/")
}

func (c *ColSchema) IsKey() bool {
	return c.PrimaryKey
}

var numericTypes = map[schema.Type]bool{
	schema.TypeFloat32: true,
	schema.TypeFloat64: true,
	schema.TypeInt64:   true,
	schema.TypeInt32:   true,
	schema.TypeInt16:   true,
	schema.TypeInt8:    true,
	schema.TypeUint64:  true,
	schema.TypeUint32:  true,
	schema.TypeUint16:  true,
	schema.TypeUint8:   true,
}

func (c *ColSchema) Numeric() bool {
	return numericTypes[schema.Type(c.DataType)]
}

func (c *ColSchema) String() string {
	return fmt.Sprintf("Col(Name:%s, Path:%s Type:%s Key:%v Required:%v)", c.ColumnName, c.ColPath(), c.DataType, c.PrimaryKey, c.Required)
}
