package dblog

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/changeitem"
)

const (
	defaultSeparator        = "#"
	FallbackChunkSize       = uint64(100_000)
	DefaultChunkSizeInBytes = uint64(10_000_000)

	AlwaysTrueWhereStatement = abstract.WhereStatement("1 = 1")
	emptySQLTuple            = "()"
)

type ChangeItemConverter func(val interface{}, colSchema abstract.ColSchema) (string, error)

func InferChunkSize(storage abstract.SampleableStorage, tableID abstract.TableID, chunkSizeInBytes uint64) (uint64, error) {
	tableSize, err := storage.TableSizeInBytes(tableID)
	if err != nil {
		return 0, xerrors.Errorf("failed to resolve table size: %w", err)
	}

	rowsCount, err := storage.EstimateTableRowsCount(tableID)
	if err != nil {
		return 0, xerrors.Errorf("failed to estimate table rows count: %w", err)
	}

	if rowsCount == 0 {
		logger.Log.Infof("EstimateTableRowsCount returned 0, choosing fallbackChunkSize: %d", FallbackChunkSize)
		return FallbackChunkSize, nil
	}

	if tableSize == 0 {
		logger.Log.Infof("TableSizeInBytes returned 0, choosing fallbackChunkSize: %d", FallbackChunkSize)
		return FallbackChunkSize, nil
	}

	avgRowSizeInBytes := tableSize / rowsCount

	return chunkSizeInBytes / avgRowSizeInBytes, nil
}

func MakeNextWhereStatement(primaryKey, lowBound []string) abstract.WhereStatement {
	if len(primaryKey) == 0 || len(lowBound) == 0 {
		return AlwaysTrueWhereStatement
	}

	sqlPrimaryKeyTuple := MakeSQLTuple(primaryKey)
	sqlLowBoundTuple := MakeSQLTuple(lowBound)

	whereStatement := abstract.WhereStatement(fmt.Sprintf("%s > %s", sqlPrimaryKeyTuple, sqlLowBoundTuple))

	return whereStatement
}

func MakeSQLTuple(stringArray []string) string {
	if len(stringArray) == 0 {
		return emptySQLTuple
	}

	return fmt.Sprintf("(%s)", strings.Join(stringArray, ","))
}

func PKeysToStringArr(item *abstract.ChangeItem, primaryKey []string, converter ChangeItemConverter) ([]string, error) {
	keyValue := make([]string, len(primaryKey))

	fastTableSchema := changeitem.MakeFastTableSchema(item.TableSchema.Columns())
	var columnNamesIndices map[string]int

	keysChanged := item.KeysChanged()
	if keysChanged {
		columnNamesIndices = make(map[string]int, len(item.OldKeys.KeyNames))

		for i, columnName := range item.OldKeys.KeyNames {
			columnNamesIndices[columnName] = i
		}
	} else {
		columnNamesIndices = item.ColumnNameIndices()
	}

	for i, key := range primaryKey {

		var itemVal interface{}
		if keysChanged {
			itemVal = item.OldKeys.KeyValues[columnNamesIndices[key]]
		} else {
			itemVal = item.ColumnValues[columnNamesIndices[key]]
		}

		itemColSchema := fastTableSchema[changeitem.ColumnName(key)]

		strVal, err := converter(itemVal, itemColSchema)
		if err != nil {
			return nil, xerrors.Errorf("failed to represent item value: %w", err)
		}

		keyValue[i] = strVal
	}

	return keyValue, nil
}

func ResolvePrimaryKeyColumns(
	ctx context.Context,
	storage abstract.Storage,
	tableID abstract.TableID,
	IsSupportedKeyType func(keyType string) bool,
) ([]string, error) {

	schema, err := storage.TableSchema(ctx, tableID)
	if err != nil {
		return nil, xerrors.Errorf("unable to get table schema tableID: %s, err: %w", tableID, err)
	}

	var primaryKey []string

	for _, column := range schema.Columns() {
		if column.PrimaryKey {
			primaryKey = append(primaryKey, column.ColumnName)
		}

		if !IsSupportedKeyType(column.OriginalType) {
			return nil, xerrors.Errorf("unsupported by data-transfer incremental snapshot")
		}
	}

	if len(primaryKey) == 0 {
		return nil, xerrors.Errorf("table %s without primary key - it's unsupported case", tableID.Name)
	}

	return primaryKey, nil
}

func stringArrToString(stringArray []string, separator string) string {
	var builder strings.Builder

	for _, str := range stringArray {
		length := strconv.Itoa(len(str))

		builder.WriteString(length)
		builder.WriteString(separator)
		builder.WriteString(str)
	}

	return builder.String()
}

func ResolveChunkMapFromArr(items []abstract.ChangeItem, primaryKey []string, converter ChangeItemConverter) (map[string]abstract.ChangeItem, error) {
	chunk := make(map[string]abstract.ChangeItem)

	for _, item := range items {
		keyValue, err := PKeysToStringArr(&item, primaryKey, converter)
		if err != nil {
			return nil, xerrors.Errorf("failed to resolve key value: %w", err)
		}

		encodedKey := stringArrToString(keyValue, defaultSeparator)

		chunk[encodedKey] = item
	}

	return chunk, nil
}

func ConvertArrayToString(array []string) (string, error) {
	jsonData, err := json.Marshal(array)
	if err != nil {
		return "", err
	}

	return string(jsonData), nil
}

func ConvertStringToArray(jsonString string) ([]string, error) {
	var array []string
	err := json.Unmarshal([]byte(jsonString), &array)
	if err != nil {
		return nil, err
	}

	return array, nil
}
