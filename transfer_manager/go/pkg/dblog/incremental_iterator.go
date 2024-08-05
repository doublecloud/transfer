package dblog

import (
	"context"

	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/dblog/tablequery"
)

type IncrementalIterator struct {
	storage     tablequery.StorageTableQueryable
	tableQuery  *tablequery.TableQuery
	signalTable SignalTable

	itemConverter changeItemConverter
	pkColNames    []string
	lowBound      []string
	limit         uint64
}

func NewIncrementalIterator(
	storage tablequery.StorageTableQueryable,
	tableQuery *tablequery.TableQuery,
	signalTable SignalTable,
	itemConverter changeItemConverter,
	pkColNames []string,
	limit uint64,
) (*IncrementalIterator, error) {
	iter := &IncrementalIterator{
		storage:       storage,
		tableQuery:    tableQuery,
		signalTable:   signalTable,
		itemConverter: itemConverter,
		pkColNames:    pkColNames,
		lowBound:      nil,
		limit:         limit,
	}

	return iter, nil
}

func (i *IncrementalIterator) Next(ctx context.Context) ([]abstract.ChangeItem, error) {
	i.tableQuery.Filter = makeNextWhereStatement(i.pkColNames, i.lowBound)

	return i.loadTablePart(ctx)
}

func (i *IncrementalIterator) loadTablePart(ctx context.Context) ([]abstract.ChangeItem, error) {
	if err := i.signalTable.CreateWatermark(ctx, i.tableQuery.TableID, LowWatermarkType); err != nil {
		return nil, xerrors.Errorf("failed to create watermark when selecting chunk")
	}

	var chunk []abstract.ChangeItem
	var lowBound []string

	chunkPusher := func(items []abstract.ChangeItem) error {
		if len(items) > 0 {
			lastItem := items[len(items)-1]
			lastKeyValue, err := pKeysToStringArr(lastItem, i.pkColNames, i.itemConverter)
			if err != nil {
				return xerrors.Errorf("unable to get key value: %w", err)
			}

			lowBound = lastKeyValue
		}

		chunk = items

		return nil
	}

	err := i.storage.LoadQueryTable(ctx, *i.tableQuery, chunkPusher)
	if err != nil {
		return nil, xerrors.Errorf("unable to load table: %s, err: %w", i.tableQuery.TableID, err)
	}

	if err = i.signalTable.CreateWatermark(ctx, i.tableQuery.TableID, HighWatermarkType); err != nil {
		return nil, xerrors.Errorf("failed to create watermark after selecting chunk")
	}

	i.lowBound = lowBound

	return chunk, nil
}
