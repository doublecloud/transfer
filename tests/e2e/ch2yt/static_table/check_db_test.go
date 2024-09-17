package snapshot

import (
	"context"
	"os"
	"testing"

	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/coordinator"
	"github.com/doublecloud/transfer/pkg/providers/clickhouse/model"
	ytcommon "github.com/doublecloud/transfer/pkg/providers/yt"
	"github.com/doublecloud/transfer/pkg/worker/tasks"
	"github.com/doublecloud/transfer/tests/helpers"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	ytcommon.InitExe()
	os.Exit(m.Run())
}

func TestClickhouseToYtStatic(t *testing.T) {
	src := &model.ChSource{
		ShardsList: []model.ClickHouseShard{
			{
				Name: "_",
				Hosts: []string{
					"localhost",
				},
			},
		},
		User:       "default",
		Password:   "",
		Database:   "mtmobproxy",
		HTTPPort:   helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT"),
		NativePort: helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT"),
	}
	src.WithDefaults()

	dstModel := &ytcommon.YtDestination{
		Path:                     "//home/cdc/tests/e2e/pg2yt/yt_static",
		Cluster:                  os.Getenv("YT_PROXY"),
		CellBundle:               "default",
		PrimaryMedium:            "default",
		Static:                   false,
		DisableDatetimeHack:      true,
		UseStaticTableOnSnapshot: false, // this test is not supposed to work for static table
	}
	dst := &ytcommon.YtDestinationWrapper{Model: dstModel}
	dst.WithDefaults()

	t.Run("activate", func(t *testing.T) {
		transfer := helpers.MakeTransfer("fake", src, dst, abstract.TransferTypeSnapshotOnly)
		require.NoError(t, tasks.ActivateDelivery(context.Background(), nil, coordinator.NewFakeClient(), *transfer, solomon.NewRegistry(solomon.NewRegistryOpts())))
		require.NoError(t, helpers.CompareStorages(t, src, dst.LegacyModel(), helpers.NewCompareStorageParams().WithEqualDataTypes(func(lDataType, rDataType string) bool {
			return true
		})))
	})
}
