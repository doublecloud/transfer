package ydb

import (
	"regexp"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

func filterYdbTableColumns(filter []YdbColumnsFilter, description options.Description) ([]options.Column, error) {
	for _, filterRule := range filter {
		tablesWithFilterRegExp, err := regexp.Compile(filterRule.TableNamesRegexp)
		if err != nil {
			return nil, xerrors.Errorf("unable to compile regexp: %s: %w", filterRule.TableNamesRegexp, err)
		}
		if !tablesWithFilterRegExp.MatchString(description.Name) {
			continue
		}
		primaryKey := map[string]bool{}
		for _, k := range description.PrimaryKey {
			primaryKey[k] = true
		}
		columnsToFilterRegExp, err := regexp.Compile(filterRule.ColumnNamesRegexp)
		if err != nil {
			return nil, xerrors.Errorf("unable to compile regexp: %s: %w", filterRule.ColumnNamesRegexp, err)
		}
		var filteredColumns = make([]options.Column, 0)
		for _, column := range description.Columns {
			var hasMatch = columnsToFilterRegExp.MatchString(column.Name)
			if (filterRule.Type == YdbColumnsWhiteList && hasMatch) ||
				(filterRule.Type == YdbColumnsBlackList && !hasMatch) {
				filteredColumns = append(filteredColumns, column)
			} else {
				if primaryKey[column.Name] {
					errorMessage := "Table loading failed. Unable to filter primary key %s of table: %s"
					return nil, xerrors.Errorf(errorMessage, column.Name, description.Name)
				}
			}
		}
		if len(filteredColumns) == 0 {
			errorMessage := "Table loading failed. Got empty list of columns after filtering: %s"
			return nil, xerrors.Errorf(errorMessage, description.Name)
		}
		return filteredColumns, nil
	}
	return description.Columns, nil
}

func flatten(batch [][]abstract.ChangeItem) []abstract.ChangeItem {
	sumSize := 0
	for _, currArr := range batch {
		sumSize += len(currArr)
	}
	result := make([]abstract.ChangeItem, 0, sumSize)
	for _, currArr := range batch {
		result = append(result, currArr...)
	}
	return result
}
