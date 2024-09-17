package abstract

import (
	"sort"

	"go.ytsaurus.tech/yt/go/schema"
)

type ColumnSchema struct {
	Name    string      `yson:"name" json:"name"`
	YTType  schema.Type `yson:"type" json:"type"`
	Primary bool        `json:"primary"`
}

func ToYtSchema(original []ColSchema, fixAnyTypeInPrimaryKey bool) []schema.Column {
	result := make([]schema.Column, len(original))
	for idx, el := range original {
		result[idx] = schema.Column{
			Name:       el.ColumnName,
			Expression: el.Expression,
			Type:       schema.Type(el.DataType),
		}
		if el.PrimaryKey {
			result[idx].SortOrder = schema.SortAscending
			if result[idx].Type == schema.TypeAny && fixAnyTypeInPrimaryKey {
				result[idx].Type = schema.TypeString // should not use any as keys
			}
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].SortOrder != schema.SortNone
	})
	return result
}

type SourceReader struct {
	TotalCount int
	Reader     chan map[string]interface{}
	Name       string
	RawSchema  []ColumnSchema
	Schema     string
	Table      string
	Lsn        uint64
	CommitTime uint64
}

type Source interface {
	Run(sink AsyncSink) error
	Stop()
}

type Fetchable interface {
	Fetch() ([]ChangeItem, error)
}
