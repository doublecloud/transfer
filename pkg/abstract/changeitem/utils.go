package changeitem

import (
	"encoding/json"
	"fmt"
)

// MakeMapColNameToIndex returns a mapping of a column name to an index in the given slice.
func MakeMapColNameToIndex(in []ColSchema) map[string]int {
	result := make(map[string]int)
	for i, column := range in {
		result[column.ColumnName] = i
	}
	return result
}

func KeyNames(in []ColSchema) []string {
	var result []string
	for _, column := range in {
		if column.IsKey() {
			result = append(result, column.ColumnName)
		}
	}
	return result
}

// Sniff dat data.
func Sniff(input []ChangeItem) string {
	tables := make(map[TableID]ChangeItem)
	tablesCntr := make(map[TableID]int)
	for _, row := range input {
		if !row.IsRowEvent() {
			continue
		}
		tid := row.TableID()
		tablesCntr[tid]++
		_, ok := tables[tid]
		if ok {
			continue
		}
		tables[tid] = row
	}
	res := ""
	for table, row := range tables {
		tableDesc := fmt.Sprintf("table: %v with %v rows", table, tablesCntr[table])
		for i, val := range row.ColumnValues {
			if len(input[0].ColumnNames) > i && len(input[0].TableSchema.Columns()) > i {
				tableDesc += fmt.Sprintf(
					"\n	Column: #%v %v = %T(%v) %v",
					i,
					input[0].ColumnNames[i],
					val,
					jsonify(val),
					input[0].TableSchema.Columns()[i].String(),
				)
			}
		}
		tableDesc += "\n"
		res += tableDesc
	}
	return res
}

func jsonify(val interface{}) string {
	data, _ := json.MarshalIndent(val, "", "   ")
	return string(data)
}

// SplitUpdatedPKeys takes a list of changes and splits them into sublists
// based on updated PKeys. Each sublist includes all the previous changes
// up to the change with updated PKey. This change split into two sub-changes
// one delete (for old row) and one insert.
// Note:
//   - The order of changes within each sublist is maintained.
func SplitUpdatedPKeys(input []ChangeItem) [][]ChangeItem {
	var result [][]ChangeItem
	var currentSequence []ChangeItem

	for _, change := range input {
		if change.KeysChanged() {
			if len(currentSequence) > 0 {
				result = append(result, currentSequence)
			}
			result = append(result, []ChangeItem{{
				ID:           change.ID,
				LSN:          change.LSN,
				CommitTime:   change.CommitTime,
				Counter:      change.Counter,
				Kind:         DeleteKind,
				Schema:       change.Schema,
				Table:        change.Table,
				PartID:       change.PartID,
				ColumnNames:  nil,
				ColumnValues: nil,
				TableSchema:  change.TableSchema,
				OldKeys:      change.OldKeys,
				TxID:         change.TxID,
				Query:        change.Query,
				Size:         change.Size,
			}, {
				ID:           change.ID,
				LSN:          change.LSN,
				CommitTime:   change.CommitTime,
				Counter:      change.Counter + 1,
				Kind:         InsertKind,
				Schema:       change.Schema,
				Table:        change.Table,
				PartID:       change.PartID,
				ColumnNames:  change.ColumnNames,
				ColumnValues: change.ColumnValues,
				TableSchema:  change.TableSchema,
				OldKeys:      EmptyOldKeys(),
				TxID:         change.TxID,
				Query:        change.Query,
				Size:         change.Size,
			}})
			currentSequence = []ChangeItem{}
		} else {
			currentSequence = append(currentSequence, change)
		}
	}

	if len(currentSequence) > 0 {
		result = append(result, currentSequence)
	}

	return result
}

func SplitByTableID(batch []ChangeItem) map[TableID][]ChangeItem {
	res := map[TableID][]ChangeItem{}
	for _, item := range batch {
		res[item.TableID()] = append(res[item.TableID()], item)
	}
	return res
}

func SplitByID(batch []ChangeItem) []TxBound {
	txs := make([]TxBound, 0)
	if len(batch) == 0 {
		return txs
	}
	currentTxID := batch[0].ID
	prevTxID := 0
	for i := range batch {
		if currentTxID != batch[i].ID {
			txs = append(txs, TxBound{prevTxID, i})
			prevTxID = i
			currentTxID = batch[i].ID
		}
	}
	txs = append(txs, TxBound{prevTxID, len(batch)})
	return txs
}
