package clickhouse

import (
	"bufio"
	"bytes"
	"time"

	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/base"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/clickhouse/format"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/clickhouse/model"
)

type HTTPEventsBatch struct {
	Data        []byte
	Cols        *abstract.TableSchema
	scanner     *bufio.Scanner
	Part        *TablePartA2
	ColNames    []string
	readerStart time.Time
	Format      model.ClickhouseIOFormat
	RowCount    int
}

func (b *HTTPEventsBatch) ColumnNames() []string {
	var res []string
	for _, col := range b.Cols.Columns() {
		res = append(res, col.ColumnName)
	}
	return res
}

func (b *HTTPEventsBatch) Next() bool {
	return b.scanner.Scan()
}

func (b *HTTPEventsBatch) Event() (base.Event, error) {
	row := b.scanner.Bytes()
	return format.NewEvent(b.Format, row, b.Cols, b.ColNames, b.Part.TableID, b.readerStart)
}

func NewHTTPEventsBatch(part *TablePartA2, data []byte, cols *abstract.TableSchema, readerStart time.Time, format model.ClickhouseIOFormat, count int) *HTTPEventsBatch {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	var colNames []string
	for _, col := range cols.Columns() {
		colNames = append(colNames, col.ColumnName)
	}
	return &HTTPEventsBatch{
		Data:        data,
		Cols:        cols,
		ColNames:    colNames,
		scanner:     scanner,
		Part:        part,
		readerStart: readerStart,
		Format:      format,
		RowCount:    count,
	}
}

func NewJSONCompactBatch(part *TablePartA2, data []byte, cols *abstract.TableSchema, readerStart time.Time) *HTTPEventsBatch {
	return NewHTTPEventsBatch(part, data, cols, readerStart, "JSONCompactEachRow", 0)
}
