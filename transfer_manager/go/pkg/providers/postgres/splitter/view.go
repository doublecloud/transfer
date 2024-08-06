package splitter

import (
	"context"
	"time"

	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

type View struct {
	storage                     postgresStorage
	snapshotDegreeOfParallelism int
}

func (t *View) Split(ctx context.Context, table abstract.TableDescription) (*SplittedTableMetadata, error) {

	logger.Log.Info("Will calculate exact view rows count", log.String("table", table.String()))

	exactRowsForQuery, err := t.storage.ExactTableDescriptionRowsCount(ctx, table, 15*time.Second)
	if err != nil {
		return nil, xerrors.Errorf("failed to calculate view rows count, table:%s, err: %w", table.Fqtn(), err)
	}

	logger.Log.Infof("Got exact rows count for view %v: %v", table.Fqtn(), exactRowsForQuery)

	return &SplittedTableMetadata{
		DataSizeInBytes: 0,
		DataSizeInRows:  exactRowsForQuery,
		PartsCount:      uint64(t.snapshotDegreeOfParallelism),
	}, nil
}

func NewView(storage postgresStorage, snapshotDegreeOfParallelism int) *View {
	return &View{
		storage:                     storage,
		snapshotDegreeOfParallelism: snapshotDegreeOfParallelism,
	}
}
