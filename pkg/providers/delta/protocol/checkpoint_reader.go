package protocol

import (
	"strings"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/providers/delta/action"
	"github.com/doublecloud/transfer/pkg/providers/delta/store"
	"github.com/doublecloud/transfer/pkg/util/iter"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/reader"
)

type CheckpointReader interface {
	Read(path string) (iter.Iter[action.Container], error)
}

func NewCheckpointReader(store store.Store) (*StoreCheckpointReader, error) {
	return &StoreCheckpointReader{store: store}, nil
}

type StoreCheckpointReader struct {
	store store.Store
}

func (l *StoreCheckpointReader) Read(path string) (iter.Iter[action.Container], error) {
	data, err := l.store.Read(path)
	if err != nil {
		return nil, xerrors.Errorf("unable to read: %s: %w", path, err)
	}

	var rows []string
	for data.Next() {
		row, err := data.Value()
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	pf := buffer.NewBufferFileFromBytes([]byte(strings.Join(rows, "\n")))
	pr, err := reader.NewParquetReader(pf, nil, 4)
	if err != nil {
		return nil, xerrors.Errorf("unable to read parquet fail: %w", err)
	}

	return &localParquetIterater{
		reader:  pr,
		numRows: pr.GetNumRows(),
		cur:     0,
	}, nil
}

type localParquetIterater struct {
	numRows int64
	cur     int64
	reader  *reader.ParquetReader
}

func (p *localParquetIterater) Next() bool {
	return p.cur < p.numRows
}

func (p *localParquetIterater) Value() (action.Container, error) {
	res := new(action.Single)
	if err := p.reader.Read(&res); err != nil {
		return nil, xerrors.Errorf("unable to read val: %w", err)
	}
	p.cur++
	return res.Unwrap(), nil
}

func (p *localParquetIterater) Close() error {
	return nil
}
