package snapshot

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/pkg/abstract"
	cpclient "github.com/doublecloud/transfer/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/providers/ydb"
	ytcommon "github.com/doublecloud/transfer/pkg/providers/yt"
	ytstorage "github.com/doublecloud/transfer/pkg/providers/yt/storage"
	"github.com/doublecloud/transfer/pkg/worker/tasks"
	"github.com/doublecloud/transfer/tests/helpers"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/schema"
)

func TestMain(m *testing.M) {
	ytcommon.InitExe()
	os.Exit(m.Run())
}

func TestGroup(t *testing.T) {
	src := &ydb.YdbSource{
		Token:              server.SecretString(os.Getenv("YDB_TOKEN")),
		Database:           helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             nil,
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           false,
		ServiceAccountID:   "",
	}
	dst := ytcommon.NewYtDestinationV1(ytcommon.YtDestination{
		Path:                     "//home/cdc/test/pg2yt_e2e",
		Cluster:                  os.Getenv("YT_PROXY"),
		CellBundle:               "default",
		PrimaryMedium:            "default",
		UseStaticTableOnSnapshot: false, // TM-4444
	})

	sourcePort, err := helpers.GetPortFromStr(src.Instance)
	require.NoError(t, err)
	targetPort, err := helpers.GetPortFromStr(dst.Cluster())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "YDB source", Port: sourcePort},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	helpers.InitSrcDst(helpers.TransferID, src, dst, abstract.TransferTypeSnapshotOnly)
	t.Run("seed data", func(t *testing.T) {
		Target := &ydb.YdbDestination{
			Database: src.Database,
			Token:    src.Token,
			Instance: src.Instance,
		}
		Target.WithDefaults()
		sinker, err := ydb.NewSinker(logger.Log, Target, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)
		testSchema := abstract.NewTableSchema([]abstract.ColSchema{
			{ColumnName: "id", DataType: string(schema.TypeInt32), PrimaryKey: true},
			{ColumnName: "val", DataType: string(schema.TypeAny), OriginalType: "ydb:Yson"},
		})
		require.NoError(t, sinker.Push([]abstract.ChangeItem{{
			Kind:         abstract.InsertKind,
			Schema:       "",
			Table:        "foo/inserts_delete_test",
			ColumnNames:  []string{"id", "val"},
			ColumnValues: []interface{}{1, map[string]interface{}{"a": 123}},
			TableSchema:  testSchema,
		}}))
	})

	t.Run("activate transfer", func(t *testing.T) {
		transfer := helpers.MakeTransfer(helpers.TransferID, src, dst, abstract.TransferTypeSnapshotOnly)
		require.NoError(t, tasks.ActivateDelivery(context.TODO(), nil, cpclient.NewStatefulFakeClient(), *transfer, helpers.EmptyRegistry()))
	})

	t.Run("check data", func(t *testing.T) {
		ytStorageParams := ytcommon.YtStorageParams{
			Token:   dst.Token(),
			Cluster: os.Getenv("YT_PROXY"),
			Path:    dst.Path(),
			Spec:    nil,
		}
		st, err := ytstorage.NewStorage(&ytStorageParams)
		require.NoError(t, err)
		var data []map[string]interface{}
		require.NoError(t, st.LoadTable(context.Background(), abstract.TableDescription{
			Name:   "foo/inserts_delete_test",
			Schema: "",
		}, func(input []abstract.ChangeItem) error {
			for _, row := range input {
				if row.Kind == abstract.InsertKind {
					data = append(data, row.AsMap())
				}
			}
			abstract.Dump(input)
			return nil
		}))
		fmt.Printf("data %v \n", data)
		require.Equal(t, data, []map[string]interface{}{
			{"id": int64(1), "val": map[string]interface{}{"a": int64(123)}},
		})
	})
}
