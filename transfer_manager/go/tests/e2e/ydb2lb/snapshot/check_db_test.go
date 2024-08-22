package mirror

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/logbroker"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/ydb"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers/lbenv"
	"github.com/stretchr/testify/require"
)

func TestPushClientLogs(t *testing.T) {
	lbEnv, stop := lbenv.NewLbEnv(t)
	defer stop()
	lbReceivingPort := lbEnv.Port

	// prepare src

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

	// prepare dst

	dst := &logbroker.LbDestination{
		Instance:    lbEnv.Endpoint,
		Topic:       "dst/topic",
		Credentials: lbEnv.ConsumerOptions().Credentials,
		Port:        lbReceivingPort,
		FormatSettings: server.SerializationFormat{
			Name: server.SerializationFormatNative,
		},
		TLS: logbroker.DisabledTLS,
	}

	// init src

	masterChangeItem := helpers.YDBInitChangeItem("dectest/timmyb32r-test")
	t.Run("init source database", func(t *testing.T) {
		Target := &ydb.YdbDestination{
			Database: src.Database,
			Token:    src.Token,
			Instance: src.Instance,
		}
		Target.WithDefaults()
		sinker, err := ydb.NewSinker(logger.Log, Target, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)
		require.NoError(t, sinker.Push([]abstract.ChangeItem{*masterChangeItem}))
	})

	// activate

	helpers.InitSrcDst(helpers.TransferID, src, dst, abstract.TransferTypeSnapshotOnly)
	transfer := helpers.MakeTransfer(helpers.TransferID, src, dst, abstract.TransferTypeSnapshotOnly)
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	// check

	dataCmp := func(in string, index int) bool {
		fmt.Printf("received string:%s\n", in)
		changeItems, err := abstract.UnmarshalChangeItems([]byte(in))
		require.NoError(t, err)
		require.Len(t, changeItems, 1)
		if changeItems[0].Kind == abstract.InsertKind {
			masterChangeItem.CommitTime = 0
			masterChangeItem.PartID = ""
			changeItems[0].CommitTime = 0
			changeItems[0].PartID = ""

			masterChangeItemStr, err := json.Marshal(masterChangeItem)
			require.NoError(t, err)

			changeItemsStr, err := json.Marshal(changeItems[0])
			require.NoError(t, err)

			fmt.Printf("master string:%s\n", masterChangeItemStr)
			fmt.Printf("_real_ string:%s\n", changeItemsStr)

			return string(masterChangeItemStr) == string(changeItemsStr)
		}
		return true
	}

	lbenv.CheckResult(t, lbEnv.Env, dst.Database, dst.Topic, 5, dataCmp, lbenv.ComparatorStub, lbenv.ComparatorStub, lbenv.ComparatorStub, lbenv.ComparatorStub)
}
