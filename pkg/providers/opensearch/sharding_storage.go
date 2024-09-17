package opensearch

import (
	"context"

	"github.com/doublecloud/transfer/pkg/abstract"
)

func (s *Storage) ShardTable(ctx context.Context, table abstract.TableDescription) ([]abstract.TableDescription, error) {
	return s.elasticShardingStorage.ShardTable(ctx, table)
}
