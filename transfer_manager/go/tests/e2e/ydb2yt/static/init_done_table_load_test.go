package static

import (
	"context"
	"os"
	"testing"

	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/ydb"
	ytcommon "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/yt"
	ytclient "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/yt/client"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
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
		Path:          "//home/cdc/test/pg2yt_e2e_static_snapshot",
		Cluster:       os.Getenv("YT_PROXY"),
		CellBundle:    "default",
		PrimaryMedium: "default",
		Static:        true,
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

	// init data
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

	// activate transfer
	transfer := helpers.MakeTransfer(helpers.TransferID, src, dst, abstract.TransferTypeSnapshotOnly)
	helpers.Activate(t, transfer)

	// check data

	// To run test locally set YT_PROXY and YT_TOKEN
	config := new(yt.Config)
	client, err := ytclient.NewYtClientWrapper(ytclient.HTTP, nil, config)
	require.NoError(t, err)

	reader, err := client.ReadTable(context.Background(), ypath.Path(dst.Path()).Child("_foo/inserts_delete_test"), nil)
	require.NoError(t, err)

	var data []map[string]interface{}
	for reader.Next() {
		var row map[string]interface{}
		err := reader.Scan(&row)
		require.NoError(t, err)
		data = append(data, row)
	}
	require.Equal(t, data, []map[string]interface{}{
		{"id": int64(1), "val": map[string]interface{}{"a": int64(123)}},
	})
}
