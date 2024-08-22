package main

import (
	"os"
	"testing"
	"time"

	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/logbroker"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/ydb"
	ytcommon "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/yt"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers/lbenv"
	"github.com/stretchr/testify/require"
)

func TestLoad(t *testing.T) {
	lbEnv, stop := lbenv.NewLbEnv(t)
	defer stop()

	//------------------------------------------------------------------------------
	// ydb -> lb

	src1 := &ydb.YdbSource{
		Token:              server.SecretString(os.Getenv("YDB_TOKEN")),
		Database:           helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             []string{"hardware/default/billing/meta/billing_accounts"},
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           false,
		ServiceAccountID:   "",
	}
	src1.WithDefaults()

	t.Run("init source database", func(t *testing.T) {
		Target := &ydb.YdbDestination{
			Database: src1.Database,
			Token:    src1.Token,
			Instance: src1.Instance,
		}
		Target.WithDefaults()
		sink, err := ydb.NewSinker(logger.Log, Target, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)

		currChangeItem := helpers.YDBInitChangeItem("hardware/default/billing/meta/billing_accounts")
		require.NoError(t, sink.Push([]abstract.ChangeItem{*currChangeItem}))
	})

	dst1 := &logbroker.LbDestination{
		Instance:        lbEnv.Endpoint,
		Topic:           lbEnv.DefaultTopic,
		Credentials:     lbEnv.ConsumerOptions().Credentials,
		WriteTimeoutSec: 60,
		Port:            lbEnv.ConsumerOptions().Port,
		FormatSettings: server.SerializationFormat{
			Name: server.SerializationFormatNative,
		},
		TLS: logbroker.DisabledTLS,
	}
	dst1.WithDefaults()

	transfer1 := helpers.MakeTransfer(helpers.TransferID+"1", src1, dst1, abstract.TransferTypeSnapshotOnly)
	_ = helpers.Activate(t, transfer1)

	//------------------------------------------------------------------------------
	// lb -> yt

	src2 := &logbroker.LbSource{
		Instance:    lbEnv.Endpoint,
		Topic:       lbEnv.DefaultTopic,
		Credentials: lbEnv.ConsumerOptions().Credentials,
		Consumer:    lbEnv.DefaultConsumer,
		Port:        lbEnv.ConsumerOptions().Port,
	}
	src2.WithDefaults()

	dst2 := ytcommon.NewYtDestinationV1(
		ytcommon.YtDestination{
			Path:          "//home/cdc/test/ydb2lb2yt",
			CellBundle:    "default",
			PrimaryMedium: "default",
			Cluster:       os.Getenv("YT_PROXY"),
			Cleanup:       server.DisabledCleanup,
		},
	)
	dst2.WithDefaults()

	transfer2 := helpers.MakeTransfer(helpers.TransferID+"2", src2, dst2, abstract.TransferTypeIncrementOnly)
	worker := helpers.Activate(t, transfer2)
	defer worker.Close(t)

	//------------------------------------------------------------------------------

	err := helpers.WaitDestinationEqualRowsCount(
		"",
		"billing_accounts",
		helpers.GetSampleableStorageByModel(t, dst2.LegacyModel()),
		60*time.Second,
		1,
	)
	require.NoError(t, err)
}
