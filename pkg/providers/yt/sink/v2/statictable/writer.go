package statictable

import (
	"context"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/changeitem"
	"github.com/doublecloud/transfer/pkg/providers/yt/sink"
	"github.com/doublecloud/transfer/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type WriterConfig struct {
	TransferID string
	TxClient   yt.Tx
	Path       ypath.Path
	Spec       map[string]interface{}
	ChunkSize  int
	Logger     log.Logger
	Metrics    *stats.SinkerStats
}

type Writer struct {
	tx yt.Tx

	writer yt.TableWriter

	logger     log.Logger
	rowsMetric func(rowCount int)
}

func (w *Writer) Write(items []changeitem.ChangeItem) error {
	fastTableSchema := items[0].TableSchema.FastColumns()
	for _, item := range items {
		if item.Kind != abstract.InsertKind {
			return xerrors.New("wrong change item kind for static table")
		}

		row := map[string]any{}
		for idx, col := range item.ColumnNames {
			colScheme, ok := fastTableSchema[abstract.ColumnName(col)]
			if !ok {
				return abstract.NewFatalError(xerrors.Errorf("unknown column name: %s", col))
			}
			var err error
			row[col], err = sink.Restore(colScheme, item.ColumnValues[idx])
			if err != nil {
				return xerrors.Errorf("cannot restore value for column '%s': %w", col, err)
			}
		}
		if err := w.writer.Write(row); err != nil {
			w.logger.Error("cannot write changeItem to static table", log.Any("table", item.Table), log.Error(err))
			return err
		}
	}
	w.rowsMetric(len(items))

	return nil
}

func (w *Writer) Commit() error {
	return w.writer.Commit()
}

func NewWriter(cfg WriterConfig) (*Writer, error) {
	tmpTablePath := makeTablePath(cfg.Path, cfg.TransferID, tmpNamePostfix)
	wr, err := yt.WriteTable(context.Background(), cfg.TxClient, tmpTablePath,
		yt.WithTableWriterConfig(cfg.Spec),
		yt.WithBatchSize(cfg.ChunkSize),
		yt.WithRetries(retriesCount),
		yt.WithExistingTable(),
		yt.WithAppend(),
	)
	if err != nil {
		return nil, err
	}

	return &Writer{
		tx:     cfg.TxClient,
		writer: wr,
		logger: cfg.Logger,

		rowsMetric: func(rowCount int) {
			cfg.Metrics.Table(cfg.Path.String(), "rows", rowCount)
		},
	}, nil
}
