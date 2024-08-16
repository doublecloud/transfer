// Used only in sorted_table
package sink

import (
	"reflect"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/util"
)

type changeItemView interface {
	keysChanged() bool
	makeOldKeys() (ytRow, error)
	makeRow() (ytRow, error)
}

type dataItemView struct {
	change  *abstract.ChangeItem
	columns *tableColumns
}

func (di *dataItemView) keysChanged() bool {
	return di.change.KeysChanged()
}

func (di *dataItemView) makeOldKeys() (ytRow, error) {
	row := ytRow{}
	for i, colName := range di.change.OldKeys.KeyNames {
		tableColumn, ok := di.columns.getByName(colName)
		if !ok {
			return nil, xerrors.Errorf("Cannot find column %s in schema %v", colName, di.columns.columns)
		}
		if tableColumn.PrimaryKey {
			row[colName] = restore(tableColumn, di.change.OldKeys.KeyValues[i])
		}
	}
	if len(row) == 0 {
		return nil, xerrors.Errorf("No old key columns found for change item %s", util.Sample(di.change.ToJSONString(), 10000))
	}
	return row, nil
}

func (di *dataItemView) makeRow() (ytRow, error) {
	hasOnlyPKey := true
	row := ytRow{}
	for i, colName := range di.change.ColumnNames {
		tableColumn, ok := di.columns.getByName(colName)
		if !ok {
			return nil, xerrors.Errorf("Cannot find column %s in schema %v", colName, di.columns.columns)
		}
		row[colName] = restore(tableColumn, di.change.ColumnValues[i])
		if !di.columns.hasKey(colName) {
			hasOnlyPKey = false
		}
	}
	if hasOnlyPKey {
		row["__dummy"] = nil
	}
	return row, nil
}

func newDataItemView(change *abstract.ChangeItem, columns *tableColumns) dataItemView {
	return dataItemView{change: change, columns: columns}
}

type indexItemView struct {
	dataView        dataItemView
	change          *abstract.ChangeItem
	oldRow          ytRow
	columns         *tableColumns
	indexColumnPos  int
	indexColumnName string
}

func (ii *indexItemView) indexColumnChanged() bool {
	if ii.change.Kind != "update" {
		return false
	}
	if ii.oldRow == nil {
		return false
	}
	indexTableColumn, ok := ii.columns.getByName(ii.indexColumnName)
	if !ok {
		return false
	}
	if ii.indexColumnPos < 0 {
		return false
	}
	newIndexValue := restore(indexTableColumn, ii.change.ColumnValues[ii.indexColumnPos])

	oldIndexValue, ok := ii.oldRow[ii.indexColumnName]
	if !ok {
		return false
	}

	return !reflect.DeepEqual(oldIndexValue, newIndexValue)
}

func (ii *indexItemView) keysChanged() bool {
	return ii.indexColumnChanged() || ii.dataView.keysChanged()
}

func (ii *indexItemView) makeOldKeys() (ytRow, error) {
	dataKeys, err := ii.dataView.makeOldKeys()
	if err != nil {
		return nil, err
	}
	oldKeys := ytRow{ii.indexColumnName: ii.oldRow[ii.indexColumnName]}
	for key, value := range dataKeys {
		oldKeys[key] = value
	}
	return oldKeys, nil
}

func (ii *indexItemView) makeRow() (ytRow, error) {
	tableColumn, ok := ii.columns.getByName(ii.indexColumnName)
	if !ok {
		return nil, xerrors.Errorf("Cannot find column %s in schema %v", ii.indexColumnName, ii.columns.columns)
	}

	row := ytRow{
		ii.indexColumnName: restore(tableColumn, ii.change.ColumnValues[ii.indexColumnPos]),
		"_dummy":           nil,
	}

	for i, colName := range ii.change.ColumnNames {
		tableColumn, ok := ii.columns.getByName(colName)
		if !ok {
			return nil, xerrors.Errorf("Cannot find column %s in schema %v", ii.indexColumnName, ii.columns.columns)
		}
		if !tableColumn.IsKey() {
			continue
		}

		row[colName] = restore(tableColumn, ii.change.ColumnValues[i])
	}
	return row, nil
}

var noIndexColumn error = xerrors.New("Index column not found")

func newIndexItemView(change *abstract.ChangeItem, columns *tableColumns, indexColName columnName, oldRow ytRow) (indexItemView, error) {
	dataView := newDataItemView(change, columns)

	if _, ok := columns.getByName(indexColName); !ok {
		return indexItemView{}, noIndexColumn
	}

	indexColumnPos := -1
	for i, colName := range change.ColumnNames {
		if colName == indexColName {
			indexColumnPos = i
			break
		}
	}

	return indexItemView{
		dataView:        dataView,
		change:          change,
		oldRow:          oldRow,
		columns:         columns,
		indexColumnPos:  indexColumnPos,
		indexColumnName: indexColName,
	}, nil
}
