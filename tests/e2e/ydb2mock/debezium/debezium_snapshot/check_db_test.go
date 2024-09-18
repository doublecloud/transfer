package main

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/library/go/test/yatest"
	"github.com/doublecloud/transfer/pkg/abstract"
	server "github.com/doublecloud/transfer/pkg/abstract/model"
	debeziumcommon "github.com/doublecloud/transfer/pkg/debezium/common"
	"github.com/doublecloud/transfer/pkg/debezium/testutil"
	"github.com/doublecloud/transfer/pkg/providers/ydb"
	"github.com/doublecloud/transfer/tests/helpers"
	"github.com/stretchr/testify/require"
)

//---------------------------------------------------------------------------------------------------------------------

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

	//-----------------------------------------------------------------------------------------------------------------

	canonizedDebeziumKeyArr, err := ioutil.ReadFile(yatest.SourcePath("transfer_manager/go/tests/e2e/ydb2mock/debezium/debezium_snapshot/testdata/change_item_key.txt"))
	require.NoError(t, err)
	canonizedDebeziumValArr, err := ioutil.ReadFile(yatest.SourcePath("transfer_manager/go/tests/e2e/ydb2mock/debezium/debezium_snapshot/testdata/change_item_val.txt"))
	require.NoError(t, err)
	canonizedDebeziumVal := string(canonizedDebeziumValArr)

	//-----------------------------------------------------------------------------------------------------------------
	// init

	t.Run("init source database", func(t *testing.T) {
		Target := &ydb.YdbDestination{
			Database: src.Database,
			Token:    src.Token,
			Instance: src.Instance,
		}
		Target.WithDefaults()
		sinker, err := ydb.NewSinker(logger.Log, Target, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)

		currChangeItem := helpers.YDBInitChangeItem("dectest/timmyb32r-test")
		require.NoError(t, sinker.Push([]abstract.ChangeItem{*currChangeItem}))
	})

	//-----------------------------------------------------------------------------------------------------------------
	// activate

	sinker := &helpers.MockSink{}
	target := server.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinker },
		Cleanup:       server.DisabledCleanup,
	}
	transfer := helpers.MakeTransfer("fake", src, &target, abstract.TransferTypeSnapshotOnly)

	var changeItems []abstract.ChangeItem
	sinker.PushCallback = func(input []abstract.ChangeItem) {
		changeItems = append(changeItems, input...)
	}

	helpers.Activate(t, transfer)

	//-----------------------------------------------------------------------------------------------------------------
	// check

	require.Equal(t, 5, len(changeItems))
	require.Equal(t, changeItems[0].Kind, abstract.InitShardedTableLoad)
	require.Equal(t, changeItems[1].Kind, abstract.InitTableLoad)
	require.Equal(t, changeItems[2].Kind, abstract.InsertKind)
	require.Equal(t, changeItems[3].Kind, abstract.DoneTableLoad)
	require.Equal(t, changeItems[4].Kind, abstract.DoneShardedTableLoad)

	logger.Log.Infof("changeItem dump: %s\n", changeItems[2].ToJSONString())

	testutil.CheckCanonizedDebeziumEvent(t, &changeItems[2], "fullfillment", "pguser", "pg", true, []debeziumcommon.KeyValue{{DebeziumKey: string(canonizedDebeziumKeyArr), DebeziumVal: &canonizedDebeziumVal}})
	changeItemBuf, err := json.Marshal(changeItems[2])
	require.NoError(t, err)
	changeItemDeserialized := helpers.UnmarshalChangeItem(t, changeItemBuf)
	testutil.CheckCanonizedDebeziumEvent(t, changeItemDeserialized, "fullfillment", "pguser", "pg", true, []debeziumcommon.KeyValue{{DebeziumKey: string(canonizedDebeziumKeyArr), DebeziumVal: &canonizedDebeziumVal}})
}
