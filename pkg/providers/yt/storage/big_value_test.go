package storage

import (
	"os"
	"strings"
	"testing"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/coordinator"
	"github.com/doublecloud/transfer/pkg/cleanup"
	"github.com/doublecloud/transfer/pkg/providers/yt"
	"github.com/doublecloud/transfer/pkg/providers/yt/sink"
	"github.com/stretchr/testify/require"
)

type TestObject struct {
	Data string `yson:"data"`
}

func TestBigValue(t *testing.T) {
	maxRetriesCount := sink.MaxRetriesCount
	sink.MaxRetriesCount = 1
	defer func() {
		sink.MaxRetriesCount = maxRetriesCount
	}()

	dstModel := yt.NewYtDestinationV1(yt.YtDestination{
		Path:          "//home/cdc/test/big_value",
		CellBundle:    "default",
		PrimaryMedium: "default",
		Cluster:       os.Getenv("YT_PROXY"),
	})
	dstModel.WithDefaults()

	changeItems := []abstract.ChangeItem{
		{
			Kind:  abstract.InsertKind,
			Table: "test_table",
			TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
				{
					ColumnName: "key",
					DataType:   "utf8",
					PrimaryKey: true,
				},
				{
					ColumnName: "value",
					DataType:   "any",
					PrimaryKey: false,
				},
			}),
			ColumnNames: []string{
				"key",
				"value",
			},
			ColumnValues: []interface{}{
				"1",
				&TestObject{
					Data: strings.Repeat("1", 16*1024*1024+1),
				},
			},
		},
	}

	t.Run("do_not_discard_big_values", func(t *testing.T) {
		sinker, err := sink.NewSinker(dstModel, "big_value", 0, logger.Log, emptyRegistry(), coordinator.NewFakeClient(), nil)
		require.NoError(t, err)
		defer cleanup.Close(sinker, logger.Log)

		err = sinker.Push(changeItems)
		require.Error(t, err)
	})

	t.Run("discard_big_values", func(t *testing.T) {
		dstModel.LegacyModel().(*yt.YtDestination).DiscardBigValues = true

		sinker, err := sink.NewSinker(dstModel, "big_value", 0, logger.Log, emptyRegistry(), coordinator.NewFakeClient(), nil)
		require.NoError(t, err)
		defer cleanup.Close(sinker, logger.Log)

		err = sinker.Push(changeItems)
		require.NoError(t, err)
	})
}
