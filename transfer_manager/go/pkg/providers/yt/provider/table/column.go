package table

import (
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/base"
	"go.ytsaurus.tech/yt/go/schema"
)

type YtColumn interface {
	base.Column
	setTable(base.Table)
	YtType() schema.ComplexType
}

type column struct {
	name       string
	ytType     schema.ComplexType
	ytCol      schema.Column
	typ        base.Type
	tbl        base.Table
	isOptional bool
}

func (c *column) Table() base.Table {
	return c.tbl
}

func (c *column) Name() string {
	return c.name
}

func (c *column) FullName() string {
	return c.name
}

func (c *column) Type() base.Type {
	return c.typ
}

func (c *column) YtType() schema.ComplexType {
	return c.ytType
}

func (c *column) Value(val interface{}) (base.Value, error) {
	panic("not implemented")
}

func (c *column) Nullable() bool {
	return c.isOptional
}

func (c *column) Key() bool {
	return c.ytCol.SortOrder != schema.SortNone
}

func (c *column) ToOldColumn() (*abstract.ColSchema, error) {
	typ, err := c.Type().ToOldType()
	if err != nil {
		return nil, err
	}
	s := abstract.NewColSchema(c.Name(), typ, false)
	s.Required = !c.isOptional
	s.PrimaryKey = c.Key()

	// TOD: TM-3226
	// Add saving of OriginalType with all nested types to make it available to restore types transformer.
	// if _, isPrimitive := c.ytType.(schema.Type); !isPrimitive {
	// 	s.OriginalType = - // Save composite YT type to OriginalType.
	// }

	return &s, nil
}

func (c *column) setTable(t base.Table) {
	c.tbl = t
}

func NewColumn(name string, typ base.Type, ytType schema.ComplexType, ytCol schema.Column, isOptional bool) YtColumn {
	return &column{
		name:       name,
		ytType:     ytType,
		ytCol:      ytCol,
		typ:        typ,
		tbl:        nil,
		isOptional: isOptional,
	}
}
