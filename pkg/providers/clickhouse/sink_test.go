package clickhouse

import (
	"fmt"
	"testing"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/pkg/abstract"
	server "github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/providers/clickhouse/model"
	"github.com/doublecloud/transfer/pkg/providers/clickhouse/sharding"
	"github.com/stretchr/testify/require"
)

var rows = []abstract.ChangeItem{
	{
		ColumnNames: []string{
			"test",
		},
		ColumnValues: []any{
			"rt3.iva-asdasd@15",
		},
	},
	{
		ColumnNames: []string{
			"test",
		},
		ColumnValues: []any{
			"rt3.myt-asdasd@15",
		},
	},
	{
		ColumnNames: []string{
			"test",
		},
		ColumnValues: []any{
			"rt3.sas-asdasd@5",
		},
	},
	{
		ColumnNames: []string{
			"test",
		},
		ColumnValues: []any{
			"rt3.man-asdasd@2",
		},
	},
}

func TestMultiShard_Push(t *testing.T) {
	q := &model.ChDestination{
		ShardsList: []model.ClickHouseShard{
			{
				Name: "s1",
				Hosts: []string{
					"h1",
				},
			},
			{
				Name: "s2",
				Hosts: []string{
					"h2",
				},
			},
			{
				Name: "s3",
				Hosts: []string{
					"h3",
				},
			},
		},
		ChClusterName: "test_cluster",
		ShardCol:      "test",
	}
	q.WithDefaults()
	sharder, err := newSinkImpl(new(server.Transfer), q.ToReplicationFromPGSinkParams(), logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), nil)
	require.NoError(t, err)

	checker := func(row abstract.ChangeItem, expected int) {
		t.Run(fmt.Sprintf("Shard From %v", row.ColumnValues[0]), func(t *testing.T) {
			idx := sharder.sharder(row)
			require.Equal(t, sharding.ShardID(expected), idx)
		})
	}
	checker(rows[0], 1)
	checker(rows[1], 2)
	checker(rows[2], 0)
	checker(rows[3], 0)
}

func TestMultiShard_Push_ManualMap(t *testing.T) {
	q := &model.ChDestination{
		ShardsList: []model.ClickHouseShard{
			{
				Name: "s1",
				Hosts: []string{
					"h1",
				},
			},
			{
				Name: "s2",
				Hosts: []string{
					"h2",
				},
			},
			{
				Name: "s3",
				Hosts: []string{
					"h3",
				},
			},
		},
		ChClusterName: "test_cluster",
		ColumnValueToShardNameList: []model.ClickHouseColumnValueToShardName{
			{
				ColumnValue: "rt3.iva-asdasd@15",
				ShardName:   "s1",
			},
			{
				ColumnValue: "rt3.myt-asdasd@15",
				ShardName:   "s2",
			},
			{
				ColumnValue: "rt3.sas-asdasd@5",
				ShardName:   "s3",
			},
			{
				ColumnValue: "rt3.man-asdasd@2",
				ShardName:   "s1",
			},
		},
		ShardCol: "test",
	}
	q.WithDefaults()
	sharder, err := newSinkImpl(new(server.Transfer), q.ToReplicationFromPGSinkParams(), logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), nil)
	require.NoError(t, err)

	checker := func(row abstract.ChangeItem, expected int) {
		t.Run(fmt.Sprintf("Shard From %v", row.ColumnValues[0]), func(t *testing.T) {
			idx := sharder.sharder(row)
			require.Equal(t, sharding.ShardID(expected), idx)
		})
	}
	for i, row := range rows {
		checker(row, i%3)
	}
}
func TestNewSink_shardRoundRobin(t *testing.T) {
	q := &model.ChDestination{
		ShardsList: []model.ClickHouseShard{
			{
				Name: "s1",
				Hosts: []string{
					"h1",
				},
			},
			{
				Name: "s2",
				Hosts: []string{
					"h2",
				},
			},
			{
				Name: "s3",
				Hosts: []string{
					"h3",
				},
			},
		},
		ShardByRoundRobin: true,
		ChClusterName:     "test_cluster",
	}
	q.WithDefaults()
	rt := &abstract.LocalRuntime{
		ShardingUpload: abstract.ShardUploadParams{
			JobCount: 10,
		},
		CurrentJob: 7,
	}
	sharder, err := newSinkImpl(new(server.Transfer), q.ToReplicationFromPGSinkParams(), logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), rt)
	require.NoError(t, err)

	for i, row := range rows {
		idx := sharder.sharder(row)
		require.Equal(t, sharding.ShardID((i+1)%len(sharder.shardMap)), idx)
	}
}

func TestNewSink_shardColumnShardingNoKey(t *testing.T) {
	q := &model.ChDestination{
		ShardsList: []model.ClickHouseShard{
			{
				Name: "s1",
				Hosts: []string{
					"h1",
				},
			},
			{
				Name: "s2",
				Hosts: []string{
					"h2",
				},
			},
			{
				Name: "s3",
				Hosts: []string{
					"h3",
				},
			},
		},
		ChClusterName: "test_cluster",
		ShardCol:      "non-existing-col",
	}
	q.WithDefaults()
	rt := &abstract.LocalRuntime{
		ShardingUpload: abstract.ShardUploadParams{
			JobCount: 10,
		},
		CurrentJob: 7,
	}
	sharder, err := newSinkImpl(new(server.Transfer), q.ToReplicationFromPGSinkParams(), logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), rt)
	require.NoError(t, err)

	for _, row := range rows {
		idx := sharder.sharder(row)
		require.Equal(t, sharding.ShardID(0), idx)
	}
}

func TestNewSink_shardColumnShardingNoKeyWithUserMapping(t *testing.T) {
	q := &model.ChDestination{
		ShardsList: []model.ClickHouseShard{
			{
				Name: "s1",
				Hosts: []string{
					"h1",
				},
			},
			{
				Name: "s2",
				Hosts: []string{
					"h2",
				},
			},
			{
				Name: "s3",
				Hosts: []string{
					"h3",
				},
			},
		},
		ChClusterName: "test_cluster",
		ShardCol:      "non-existing-col",
		ColumnValueToShardNameList: []model.ClickHouseColumnValueToShardName{
			{
				ColumnValue: "rt3.iva-asdasd@15",
				ShardName:   "s1",
			},
			{
				ColumnValue: "rt3.myt-asdasd@15",
				ShardName:   "s2",
			},
			{
				ColumnValue: "rt3.sas-asdasd@5",
				ShardName:   "s3",
			},
			{
				ColumnValue: "rt3.man-asdasd@2",
				ShardName:   "s1",
			},
		},
	}
	q.WithDefaults()
	rt := &abstract.LocalRuntime{
		ShardingUpload: abstract.ShardUploadParams{
			JobCount: 10,
		},
		CurrentJob: 7,
	}
	sharder, err := newSinkImpl(new(server.Transfer), q.ToReplicationFromPGSinkParams(), logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), rt)
	require.NoError(t, err)

	for _, row := range rows {
		idx := sharder.sharder(row)
		require.Equal(t, sharding.ShardID(0), idx)
	}
}
