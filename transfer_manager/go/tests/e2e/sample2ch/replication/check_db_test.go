package replication

import (
	"testing"
	"time"

	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	chrecipe "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/clickhouse/recipe"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/sample"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/stretchr/testify/require"
)

const minNumberOfRows = 400

var (
	schemaName   = "mtmobproxy"
	TransferType = abstract.TransferTypeIncrementOnly
	Source       = *sample.RecipeSource()
	Target       = *chrecipe.MustTarget(chrecipe.WithInitFile("dump/dst.sql"), chrecipe.WithDatabase(schemaName), chrecipe.WithPrefix("DB0_"))
)

func TestReplication(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "CH target", Port: Target.NativePort},
		))
	}()
	Target.WithDefaults()
	Target.Cleanup = server.DisabledCleanup

	Source.WithDefaults()
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType)

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)

	helpers.Activate(t, transfer)
	require.NoError(t, helpers.WaitCond(60*time.Second, func() bool {
		storage := helpers.GetSampleableStorageByModel(t, &Target)
		tableDescription := abstract.TableDescription{Name: Source.SampleType, Schema: schemaName}
		rowsInSrc, err := storage.ExactTableRowsCount(tableDescription.ID())
		if err != nil {
			logger.Log.Errorf("reading number of rows from schema: %v, table: %v and occured error: %v", schemaName, Source.SampleType, err)
			return false
		}
		logger.Log.Infof("number of rows in clickhouse %v", rowsInSrc)
		// minimum number of rows counted according to sampleSource defaults
		// maximumSleepTime = 2*minimumSleepTime = 200ms
		// overall in every asyncPush 128 rows
		return rowsInSrc > minNumberOfRows
	}))

}
