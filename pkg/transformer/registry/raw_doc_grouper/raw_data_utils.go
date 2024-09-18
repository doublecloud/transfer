package rawdocgrouper

import (
	"strings"

	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/util"
	"go.ytsaurus.tech/yt/go/schema"
	"golang.org/x/exp/slices"
)

const etlUpdatedField = "etl_updated_at"
const rawDataField = "doc"
const deletedField = "deleted_flg"

const pgPrefix = "pg:"
const pgText = "pg:text"
const pgTimestamp = "pg:timestamp without time zone"
const pgJSON = "pg:json"

func MakeStubFieldWithOriginalType(colName string, ytType schema.Type, primaryKey bool, original abstract.TableColumns) abstract.ColSchema {
	field := abstract.MakeTypedColSchema(colName, ytType.String(), primaryKey)
	originalType := getOriginalType(original, ytType)
	if originalType != "" {
		field.OriginalType = originalType
	}
	return field
}

func getOriginalType(original abstract.TableColumns, ytType schema.Type) string {
	for _, column := range original {
		//checking if data is from pg and at least 1 original type is present
		//in this case setting pg original for our column
		if strings.Contains(column.OriginalType, pgPrefix) {
			return getPgTypeFor(ytType)
		}
	}
	return ""
}

func getPgTypeFor(ytType schema.Type) string {
	if ytType == schema.TypeAny {
		return pgJSON
	}

	if ytType == schema.TypeDatetime || ytType == schema.TypeTimestamp {
		return pgTimestamp
	}
	//may be throw exception here? then need to change protocol
	return pgText
}

func CollectFieldsForTransformer(fields []string, columns abstract.TableColumns, isKey bool, colNameToIdx map[string]int,
	fieldsMap map[string]schema.Type) []abstract.ColSchema {
	result := make([]abstract.ColSchema, 0, len(fields))
	for _, key := range fields {
		var keySchema abstract.ColSchema
		if slices.Contains(columns.ColumnNames(), key) {
			keySchema = columns[colNameToIdx[key]]
			keySchema.PrimaryKey = isKey
			result = append(result, keySchema)
		} else if fieldType, ok := fieldsMap[key]; ok {
			keySchema = MakeStubFieldWithOriginalType(key, fieldType, isKey, columns)
			result = append(result, keySchema)
		}
	}
	return result
}

func allFieldsPresent(colNames []string, rawDocFields map[string]schema.Type, fields []string) bool {
	colSet := util.NewSet[string](colNames...)
	for _, key := range fields {
		if _, ok := rawDocFields[key]; !ok && !colSet.Contains(key) {
			return false
		}
	}
	return true
}
