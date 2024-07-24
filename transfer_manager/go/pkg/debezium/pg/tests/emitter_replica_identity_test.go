package tests

import (
	"os"
	"testing"
	"time"

	"github.com/doublecloud/tross/library/go/test/yatest"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/debezium"
	debeziumcommon "github.com/doublecloud/tross/transfer_manager/go/pkg/debezium/common"
	debeziumparameters "github.com/doublecloud/tross/transfer_manager/go/pkg/debezium/parameters"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/debezium/testutil"
	"github.com/stretchr/testify/require"
)

func TestReplicaIdentityFullUpdate(t *testing.T) {
	pgUpdateChangeItem, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/pkg/debezium/pg/tests/testdata/emitter_replica_identity__canon_change_item_update.txt"))
	require.NoError(t, err)
	originalChangeItem, err := abstract.UnmarshalChangeItem(pgUpdateChangeItem)
	require.NoError(t, err)

	emitter, err := debezium.NewMessagesEmitter(map[string]string{
		debeziumparameters.DatabaseDBName:   "public",
		debeziumparameters.TopicPrefix:      "fullfillment",
		debeziumparameters.AddOriginalTypes: "false",
		debeziumparameters.SourceType:       "pg",
	}, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)
	emitter.TestSetIgnoreUnknownSources(true)
	currDebeziumKV, err := emitter.EmitKV(originalChangeItem, time.Time{}, true, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(currDebeziumKV))

	debeziumKey, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/pkg/debezium/pg/tests/testdata/emitter_replica_identity__debezium_update_key.txt"))
	require.NoError(t, err)
	debeziumKeyStr := string(debeziumKey)
	debeziumVal, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/pkg/debezium/pg/tests/testdata/emitter_replica_identity__debezium_update_val.txt"))
	require.NoError(t, err)
	debeziumValStr := string(debeziumVal)

	testSuite := []debeziumcommon.ChangeItemCanon{{
		ChangeItem: originalChangeItem,
		DebeziumEvents: []debeziumcommon.KeyValue{
			{
				DebeziumKey: debeziumKeyStr,
				DebeziumVal: &debeziumValStr,
			},
		},
	}}

	testSuite = testutil.FixTestSuite(t, testSuite, "fullfillment", "pguser", "pg")
	for _, testCase := range testSuite {
		testutil.CheckCanonizedDebeziumEvent(t, testCase.ChangeItem, "fullfillment", "public", "pg", false, currDebeziumKV)
	}
}

func TestReplicaIdentityFullDelete(t *testing.T) {
	pgDeleteChangeItem, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/pkg/debezium/pg/tests/testdata/emitter_replica_identity__canon_change_item_delete.txt"))
	require.NoError(t, err)
	originalChangeItem, err := abstract.UnmarshalChangeItem(pgDeleteChangeItem)
	require.NoError(t, err)

	emitter, err := debezium.NewMessagesEmitter(map[string]string{
		debeziumparameters.DatabaseDBName:   "public",
		debeziumparameters.TopicPrefix:      "fullfillment",
		debeziumparameters.AddOriginalTypes: "false",
		debeziumparameters.SourceType:       "pg",
	}, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)
	emitter.TestSetIgnoreUnknownSources(true)
	currDebeziumKV, err := emitter.EmitKV(originalChangeItem, time.Time{}, true, nil)
	require.NoError(t, err)
	require.Equal(t, 2, len(currDebeziumKV))

	debeziumKey, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/pkg/debezium/pg/tests/testdata/emitter_replica_identity__debezium_delete_key.txt"))
	require.NoError(t, err)
	debeziumKeyStr := string(debeziumKey)
	debeziumVal, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/pkg/debezium/pg/tests/testdata/emitter_replica_identity__debezium_delete_val.txt"))
	require.NoError(t, err)
	debeziumValStr := string(debeziumVal)

	testSuite := []debeziumcommon.ChangeItemCanon{{
		ChangeItem: originalChangeItem,
		DebeziumEvents: []debeziumcommon.KeyValue{
			{
				DebeziumKey: debeziumKeyStr,
				DebeziumVal: &debeziumValStr,
			},
			{
				DebeziumKey: debeziumKeyStr,
				DebeziumVal: nil,
			},
		},
	}}

	testSuite = testutil.FixTestSuite(t, testSuite, "fullfillment", "pguser", "pg")
	for _, testCase := range testSuite {
		testutil.CheckCanonizedDebeziumEvent(t, testCase.ChangeItem, "fullfillment", "public", "pg", false, currDebeziumKV)
	}
}
