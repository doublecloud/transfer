package sink

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/cenkalti/backoff/v4"
	"github.com/doublecloud/tross/library/go/core/metrics/solomon"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/internal/metrics"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	client2 "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/coordinator"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/yt"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/stats"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/ypath"
	ytsdk "go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

var (
	tableSchema = abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "_partition", DataType: "string"},
		{ColumnName: "_offset", DataType: "uint64"},
		{ColumnName: "value", DataType: "string"},
	})
)

var (
	testDirPath   = ypath.Path("//home/cdc/test/ordered")
	testTablePath = yt.SafeChild(testDirPath, "test_table")
)

type testRow struct {
	Partition string `yson:"_partition"`
	Offset    uint64 `yson:"_offset"`
	Value     string `yson:"value"`
	TabletIDX int64  `yson:"$tablet_index"`
}

func TestOrderedTablet_Write(t *testing.T) {
	env, cancel := yttest.NewEnv(t)
	defer cancel()
	defer teardown(env.YT, testDirPath)
	destination := yt.NewYtDestinationV1(yt.YtDestination{
		Atomicity:     ytsdk.AtomicityFull,
		CellBundle:    "default",
		PrimaryMedium: "default",
	})
	destination.WithDefaults()
	table, err := NewOrderedTable(
		env.YT,
		testTablePath,
		tableSchema.Columns(),
		destination,
		stats.NewSinkerStats(solomon.NewRegistry(solomon.NewRegistryOpts())),
		logger.Log,
	)
	require.NoError(t, err)
	// initial load
	err = table.Write(generateBullets(2, 10))
	require.NoError(t, err)
	// fully deduplicated
	err = table.Write(generateBullets(1, 5))
	require.NoError(t, err)
	// enlarge tablets count
	err = table.Write(generateBullets(3, 15))
	require.NoError(t, err)
	rows, err := env.YT.SelectRows(
		env.Ctx,
		fmt.Sprintf("* from [%v]", testTablePath),
		nil,
	)
	require.NoError(t, err)
	tablets := map[int64][]testRow{}
	for rows.Next() {
		var row testRow
		require.NoError(t, rows.Scan(&row))
		tablets[row.TabletIDX] = append(tablets[row.TabletIDX], row)
	}

	require.Equal(t, 5, len(tablets[1]))
	require.Equal(t, 10, len(tablets[2]))
	require.Equal(t, 15, len(tablets[3]))
}

func TestOrderedTablet_ConcurrentWrite(t *testing.T) {
	env, cancel := yttest.NewEnv(t)
	defer cancel()
	defer teardown(env.YT, testDirPath)
	destination := yt.NewYtDestinationV1(yt.YtDestination{
		Atomicity:     ytsdk.AtomicityFull,
		CellBundle:    "default",
		PrimaryMedium: "default",
	})
	destination.WithDefaults()
	table, err := NewOrderedTable(
		env.YT,
		testTablePath,
		tableSchema.Columns(),
		destination,
		stats.NewSinkerStats(solomon.NewRegistry(solomon.NewRegistryOpts())),
		logger.Log,
	)
	require.NoError(t, err)
	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		defer wg.Done()
		_ = backoff.Retry(func() error {
			err := table.Write(generateBullets(2, 10))
			return err
		}, backoff.NewExponentialBackOff())
	}()
	go func() {
		defer wg.Done()
		_ = backoff.Retry(func() error {
			err := table.Write(generateBullets(1, 5))
			return err
		}, backoff.NewExponentialBackOff())
	}()
	go func() {
		defer wg.Done()
		_ = backoff.Retry(func() error {
			err := table.Write(generateBullets(3, 15))
			return err
		}, backoff.NewExponentialBackOff())
	}()
	wg.Wait()
	rows, err := env.YT.SelectRows(
		env.Ctx,
		fmt.Sprintf("* from [%v]", testTablePath),
		nil,
	)
	require.NoError(t, err)
	tablets := map[int64][]testRow{}
	for rows.Next() {
		var row testRow
		require.NoError(t, rows.Scan(&row))
		tablets[row.TabletIDX] = append(tablets[row.TabletIDX], row)
	}

	require.Equal(t, 5, len(tablets[1]))
	require.Equal(t, 10, len(tablets[2]))
	require.Equal(t, 15, len(tablets[3]))
}

func TestOrderedTable_CustomAttributes(t *testing.T) {
	env, cancel := yttest.NewEnv(t)
	defer cancel()
	defer teardown(env.YT, testDirPath)
	cfg := yt.NewYtDestinationV1(yt.YtDestination{
		Atomicity:        ytsdk.AtomicityFull,
		CellBundle:       "default",
		PrimaryMedium:    "default",
		Ordered:          true,
		CustomAttributes: map[string]string{"test": "%true"},
		Path:             testDirPath.String(),
		Cluster:          os.Getenv("YT_PROXY"),
	})
	cfg.WithDefaults()
	table, err := newSinker(cfg, "some_uniq_transfer_id", 0, logger.Log, metrics.NewRegistry(), client2.NewFakeClient())
	require.NoError(t, err)
	require.NoError(t, table.Push(generateBullets(2, 10)))
	var data bool
	require.NoError(t, env.YT.GetNode(env.Ctx, ypath.Path(fmt.Sprintf("%s/@test", testTablePath.String())), &data, nil))
	require.Equal(t, true, data)
}

func generateBullets(partNum, count int) []abstract.ChangeItem {
	res := make([]abstract.ChangeItem, 0)
	dc := []string{"sas", "vla", "man", "iva", "myt"}[partNum%5]
	part := abstract.NewPartition(fmt.Sprintf("rt3.%s--yabs-rt--bs-tracking-log", dc), 0).String()
	for j := 0; j < count; j++ {
		item := abstract.ChangeItem{
			ColumnNames: []string{"_partition", "_offset", "value"},
			ColumnValues: []interface{}{
				part,
				uint64(j),
				fmt.Sprintf("%v_%v", partNum, j),
			},
			TableSchema: tableSchema,
			Table:       "test_table",
			Kind:        abstract.InsertKind,
		}
		res = append(res, item)
	}
	return res
}

func Test_getTabletIndexByPartition(t *testing.T) {
	q, err := getTabletIndexByPartition(abstract.NewPartition("rt3.vla--yabs-rt--bs-tracking-log", 2))
	require.NoError(t, err)
	require.Equal(t, uint32(11), q)
}
