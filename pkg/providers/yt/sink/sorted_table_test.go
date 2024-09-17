package sink

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/internal/metrics"
	"github.com/doublecloud/transfer/pkg/abstract"
	client2 "github.com/doublecloud/transfer/pkg/abstract/coordinator"
	yt2 "github.com/doublecloud/transfer/pkg/providers/yt"
	"github.com/doublecloud/transfer/pkg/stats"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

func teardown(client yt.Client, path ypath.Path) {
	err := client.RemoveNode(
		context.Background(),
		path,
		&yt.RemoveNodeOptions{
			Recursive: true,
			Force:     true,
		},
	)
	if err != nil {
		logger.Log.Error("unable to delete test folder", log.Error(err))
	}
}

func TestInsertWithFloat(t *testing.T) {
	env, cancel := yttest.NewEnv(t)
	defer cancel()
	defer teardown(env.YT, "//home/cdc/test/generic/temp")
	schema_ := abstract.NewTableSchema([]abstract.ColSchema{
		{
			DataType:   "double",
			ColumnName: "test",
			PrimaryKey: true,
		},
	})
	cfg := yt2.NewYtDestinationV1(yt2.YtDestination{CellBundle: "default", PrimaryMedium: "default"})
	cfg.WithDefaults()
	table, err := NewSortedTable(env.YT, "//home/cdc/test/generic/temp", schema_.Columns(), cfg, stats.NewSinkerStats(metrics.NewRegistry()), logger.Log)
	require.NoError(t, err)
	err = table.Write([]abstract.ChangeItem{
		{
			TableSchema:  schema_,
			Kind:         "insert",
			ColumnNames:  []string{"test"},
			ColumnValues: []interface{}{3.99},
		},
	})

	if err != nil {
		t.Errorf("Unable to write %v", err)
	}
}

func TestCustomAttributes(t *testing.T) {
	env, cancel := yttest.NewEnv(t)
	defer cancel()
	defer teardown(env.YT, "//home/cdc/test/generic/temp")
	schema_ := abstract.NewTableSchema([]abstract.ColSchema{
		{
			DataType:   "double",
			ColumnName: "test",
			PrimaryKey: true,
		},
	})
	cfg := yt2.NewYtDestinationV1(yt2.YtDestination{
		Atomicity:        yt.AtomicityFull,
		CellBundle:       "default",
		PrimaryMedium:    "default",
		CustomAttributes: map[string]string{"test": "%true"},
		Path:             "//home/cdc/test/generic/temp",
		Cluster:          os.Getenv("YT_PROXY")},
	)
	cfg.WithDefaults()
	table, err := newSinker(cfg, "some_uniq_transfer_id", 0, logger.Log, metrics.NewRegistry(), client2.NewFakeClient())
	require.NoError(t, err)
	require.NoError(t, table.Push([]abstract.ChangeItem{
		{
			TableSchema:  schema_,
			Kind:         "insert",
			ColumnNames:  []string{"test"},
			ColumnValues: []interface{}{3.99},
			Table:        "test_table",
		},
	}))
	var data bool
	require.NoError(t, env.YT.GetNode(env.Ctx, ypath.Path("//home/cdc/test/generic/temp/test_table/@test"), &data, nil))
	require.Equal(t, true, data)
}

func TestSortedTable_Write_With_Indexes(t *testing.T) {
	env, cancel := yttest.NewEnv(t)
	defer cancel()
	defer teardown(env.YT, "//home/cdc/test/generic/temp")
	schema_ := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "key", DataType: "string", PrimaryKey: true},
		{ColumnName: "sub_key_1", DataType: "string"},
		{ColumnName: "sub_key_2", DataType: "string"},
		{ColumnName: "value", DataType: "string"},
	})

	cfg := yt2.NewYtDestinationV1(yt2.YtDestination{Index: []string{"sub_key_1", "sub_key_2"}, TimeShardCount: 0, CellBundle: "default", PrimaryMedium: "default"})
	cfg.WithDefaults()
	table, err := NewSortedTable(env.YT, "//home/cdc/test/generic/temp", schema_.Columns(), cfg, stats.NewSinkerStats(metrics.NewRegistry()), logger.Log)
	require.NoError(t, err)
	bulletCount := 10 * 1000
	var items []abstract.ChangeItem
	for i := 0; i < bulletCount; i++ {
		items = append(items, abstract.ChangeItem{
			Kind:        "insert",
			ColumnNames: []string{"key", "sub_key_1", "sub_key_2", "value"},
			ColumnValues: []interface{}{
				fmt.Sprintf("key-%v", i),
				fmt.Sprintf("sub-key-1-%v", i),
				fmt.Sprintf("sub-key-2-%v", i),
				fmt.Sprintf("val-%v", i),
			},
			TableSchema: schema_,
		})
	}
	chunkSize := int(cfg.ChunkSize()) / (len(cfg.Index()) + 1)
	for i := 0; i < len(items); i += chunkSize {
		end := i + chunkSize

		if end > len(items) {
			end = len(items)
		}
		err = table.Write(items[i:end])
		require.NoError(t, err)
	}
	type counter struct {
		Count int64 `yson:"count"`
	}
	rows, err := env.YT.SelectRows(
		env.Ctx,
		"sum(1) as count from [//home/cdc/test/generic/temp] group by 1",
		nil,
	)
	require.NoError(t, err)
	for rows.Next() {
		var c counter
		require.NoError(t, rows.Scan(&c))
		require.Equal(t, int64(bulletCount), c.Count)
	}
	rows, err = env.YT.SelectRows(
		env.Ctx,
		"sum(1) as count from [//home/cdc/test/generic/temp__idx_sub_key_1] group by 1",
		nil,
	)
	require.NoError(t, err)
	for rows.Next() {
		var c counter
		require.NoError(t, rows.Scan(&c))
		require.Equal(t, int64(bulletCount), c.Count)
	}
	rows, err = env.YT.SelectRows(
		env.Ctx,
		"sum(1) as count from [//home/cdc/test/generic/temp__idx_sub_key_2] group by 1",
		nil,
	)
	require.NoError(t, err)
	for rows.Next() {
		var c counter
		require.NoError(t, rows.Scan(&c))
		require.Equal(t, int64(bulletCount), c.Count)
	}
}

func TestIsSuperset(t *testing.T) {
	a := schema.Schema{
		UniqueKeys: true,
		Columns: []schema.Column{
			{
				Name:      "key",
				Type:      schema.TypeInt64,
				Required:  true,
				SortOrder: schema.SortAscending,
			},
			{
				Name:     "value",
				Type:     schema.TypeString,
				Required: false,
			},
			{
				Name:     "extra",
				Type:     schema.TypeString,
				Required: false,
			},
		},
	}
	b := schema.Schema{
		UniqueKeys: true,
		Columns: []schema.Column{
			{
				Name:      "key",
				Type:      schema.TypeInt64,
				Required:  true,
				SortOrder: schema.SortAscending,
			},
			{
				Name:     "value",
				Type:     schema.TypeString,
				Required: false,
			},
		},
	}
	require.True(t, isSuperset(a, a))
	require.True(t, isSuperset(a, b))
	require.False(t, isSuperset(b, a))
	require.True(t, isSuperset(b, b))

	a = schema.Schema{
		UniqueKeys: true,
		Columns: []schema.Column{
			{
				Name:      "key",
				Type:      schema.TypeInt64,
				Required:  true,
				SortOrder: schema.SortAscending,
			},
			{
				Name:     "extra",
				Type:     schema.TypeString,
				Required: false,
			},
			{
				Name:     "value",
				Type:     schema.TypeString,
				Required: false,
			},
		},
	}
	b = schema.Schema{
		UniqueKeys: true,
		Columns: []schema.Column{
			{
				Name:      "key",
				Type:      schema.TypeInt64,
				Required:  true,
				SortOrder: schema.SortAscending,
			},
			{
				Name:     "value",
				Type:     schema.TypeString,
				Required: false,
			},
		},
	}
	require.True(t, isSuperset(a, a))
	require.True(t, isSuperset(a, b))
	require.False(t, isSuperset(b, a))
	require.True(t, isSuperset(b, b))

	a = schema.Schema{
		UniqueKeys: true,
		Columns: []schema.Column{
			{
				Name:      "key",
				Type:      schema.TypeInt64,
				Required:  true,
				SortOrder: schema.SortAscending,
			},
			{
				Name:     "kek",
				Type:     schema.TypeString,
				Required: false,
			},
			{
				Name:     "value",
				Type:     schema.TypeString,
				Required: false,
			},
		},
	}
	b = schema.Schema{
		UniqueKeys: true,
		Columns: []schema.Column{
			{
				Name:      "key",
				Type:      schema.TypeInt64,
				Required:  true,
				SortOrder: schema.SortAscending,
			},
			{
				Name:     "lel",
				Type:     schema.TypeString,
				Required: false,
			},
			{
				Name:     "value",
				Type:     schema.TypeString,
				Required: false,
			},
		},
	}
	require.True(t, isSuperset(a, a))
	require.False(t, isSuperset(a, b))
	require.False(t, isSuperset(b, a))
	require.True(t, isSuperset(b, b))

	a = schema.Schema{
		UniqueKeys: true,
		Columns: []schema.Column{
			{
				Name:      "key",
				Type:      schema.TypeInt64,
				Required:  true,
				SortOrder: schema.SortAscending,
			},
			{
				Name:     "kek",
				Type:     schema.TypeString,
				Required: false,
			},
			{
				Name:     "value",
				Type:     schema.TypeString,
				Required: false,
			},
		},
	}
	b = schema.Schema{
		UniqueKeys: true,
		Columns: []schema.Column{
			{
				Name:      "key",
				Type:      schema.TypeInt64,
				Required:  true,
				SortOrder: schema.SortAscending,
			},
			{
				Name:     "kek",
				Type:     schema.TypeBoolean,
				Required: false,
			},
			{
				Name:     "value",
				Type:     schema.TypeString,
				Required: false,
			},
		},
	}
	require.True(t, isSuperset(a, a))
	require.False(t, isSuperset(a, b))
	require.False(t, isSuperset(b, a))
	require.True(t, isSuperset(b, b))

	a = schema.Schema{
		UniqueKeys: true,
		Columns: []schema.Column{
			{
				Name:      "key",
				Type:      schema.TypeInt64,
				Required:  true,
				SortOrder: schema.SortAscending,
			},
			{
				Name:     "kek",
				Type:     schema.TypeString,
				Required: false,
			},
			{
				Name:     "value",
				Type:     schema.TypeString,
				Required: false,
			},
		},
	}
	b = schema.Schema{
		UniqueKeys: true,
		Columns: []schema.Column{
			{
				Name:      "key",
				Type:      schema.TypeInt64,
				Required:  true,
				SortOrder: schema.SortAscending,
			},
			{
				Name:     "value",
				Type:     schema.TypeString,
				Required: false,
			},
			{
				Name:     "kek",
				Type:     schema.TypeBoolean,
				Required: false,
			},
		},
	}
	require.False(t, isSuperset(a, b))
	require.True(t, isSuperset(a, a))
	require.True(t, isSuperset(b, b))
	require.False(t, isSuperset(b, a))

	a = schema.Schema{
		UniqueKeys: false,
		Columns: []schema.Column{
			{
				Name:      "key",
				Type:      schema.TypeInt64,
				Required:  true,
				SortOrder: schema.SortAscending,
			},
			{
				Name:     "value",
				Type:     schema.TypeString,
				Required: false,
			},
		},
	}
	b = schema.Schema{
		UniqueKeys: true,
		Columns: []schema.Column{
			{
				Name:      "key",
				Type:      schema.TypeInt64,
				Required:  true,
				SortOrder: schema.SortAscending,
			},
			{
				Name:     "value",
				Type:     schema.TypeString,
				Required: false,
			},
		},
	}
	require.False(t, isSuperset(a, b))
	require.True(t, isSuperset(a, a))
	require.True(t, isSuperset(b, b))
	require.False(t, isSuperset(b, a))
}
