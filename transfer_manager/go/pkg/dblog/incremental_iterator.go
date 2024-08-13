package dblog

import (
	"context"

	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/dblog/tablequery"
	"github.com/google/uuid"
)

type IncrementalIterator struct {
	storage     tablequery.StorageTableQueryable
	tableQuery  *tablequery.TableQuery
	signalTable SignalTable

	itemConverter changeItemConverter
	pkColNames    []string
	lowBound      []string
	limit         uint64

	LowWatermarkUUID  uuid.UUID
	HighWatermarkUUID uuid.UUID

	betweenMarksOpts []func()
}

func NewIncrementalIterator(
	storage tablequery.StorageTableQueryable,
	tableQuery *tablequery.TableQuery,
	signalTable SignalTable,
	itemConverter changeItemConverter,
	pkColNames []string,
	lowBound []string,
	limit uint64,
	betweenMarksOpts ...func(),
) (*IncrementalIterator, error) {
	iter := &IncrementalIterator{
		storage:           storage,
		tableQuery:        tableQuery,
		signalTable:       signalTable,
		itemConverter:     itemConverter,
		pkColNames:        pkColNames,
		lowBound:          lowBound,
		limit:             limit,
		LowWatermarkUUID:  uuid.New(),
		HighWatermarkUUID: uuid.New(),
		betweenMarksOpts:  betweenMarksOpts,
	}

	return iter, nil
}

func (i *IncrementalIterator) Next(ctx context.Context) ([]abstract.ChangeItem, error) {
	i.tableQuery.Filter = makeNextWhereStatement(i.pkColNames, i.lowBound)

	return i.loadTablePart(ctx)
}

func (i *IncrementalIterator) loadTablePart(ctx context.Context) ([]abstract.ChangeItem, error) {
	lowWatermarkUUID, err := i.signalTable.CreateWatermark(ctx, i.tableQuery.TableID, LowWatermarkType, i.lowBound)
	if err != nil {
		return nil, xerrors.Errorf("Failed to create watermark when selecting chunk: %w", err)
	}

	i.LowWatermarkUUID = lowWatermarkUUID

	var chunk []abstract.ChangeItem

	chunkPusher := func(items []abstract.ChangeItem) error {
		if len(items) > 0 {
			lastItem := items[len(items)-1]
			lastKeyValue, err := pKeysToStringArr(&lastItem, i.pkColNames, i.itemConverter)
			if err != nil {
				return xerrors.Errorf("unable to get key value: %w", err)
			}

			i.lowBound = lastKeyValue
		}

		chunk = items

		return nil
	}

	if err := i.storage.LoadQueryTable(ctx, *i.tableQuery, chunkPusher); err != nil {
		return nil, xerrors.Errorf("unable to load table: %s, err: %w", i.tableQuery.TableID, err)
	}

	if len(chunk) != 0 {
		for _, opt := range i.betweenMarksOpts {
			opt()
		}
	}

	highWatermarkUUID, err := i.signalTable.CreateWatermark(ctx, i.tableQuery.TableID, HighWatermarkType, i.lowBound)
	if err != nil {
		return nil, xerrors.Errorf("Failed to create watermark when selecting chunk")
	}

	i.HighWatermarkUUID = highWatermarkUUID

	return chunk, nil
}
