package replication

import (
	"strconv"
	"testing"
	"time"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/pkg/abstract"
	cpclient "github.com/doublecloud/transfer/pkg/abstract/coordinator"
	"github.com/doublecloud/transfer/pkg/providers/clickhouse/model"
	chrecipe "github.com/doublecloud/transfer/pkg/providers/clickhouse/recipe"
	"github.com/doublecloud/transfer/pkg/providers/kinesis"
	"github.com/doublecloud/transfer/pkg/runtime/local"
	"github.com/doublecloud/transfer/tests/canon/reference"
	"github.com/doublecloud/transfer/tests/helpers"
	"github.com/doublecloud/transfer/tests/tcrecipes"
	"github.com/stretchr/testify/require"
)

func init() {

}

func TestReplication(t *testing.T) {
	if !tcrecipes.Enabled() {
		t.Skip()
	}

	var (
		databaseName = "public"
		transferType = abstract.TransferTypeIncrementOnly
		source       = kinesis.MustSource()
		target       = chrecipe.MustTarget(
			chrecipe.WithInitDir("dump/ch"),
			chrecipe.WithDatabase(databaseName))
	)

	helpers.InitSrcDst(helpers.TransferID, source, target, transferType)

	defer func() {
		p := source.Endpoint[len(source.Endpoint)-4:]
		port, err := strconv.Atoi(p)
		require.NoError(t, err)

		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{
				Label: "Kinesis source",
				Port:  port,
			},
			helpers.LabeledPort{
				Label: "CH target Native",
				Port:  target.NativePort,
			},
		))
	}()

	transfer := helpers.MakeTransfer(
		helpers.TransferID,
		source,
		target,
		transferType,
	)

	c := cpclient.NewStatefulFakeClient()
	localWorker := local.NewLocalWorker(
		c,
		transfer,
		helpers.EmptyRegistry(),
		logger.Log,
	)
	localWorker.Start()
	defer localWorker.Stop()

	require.NoError(t, kinesis.PutRecord(
		source,
		[]byte("Hello World!"),
		"test",
	))
	require.NoError(t, kinesis.PutRecord(
		source,
		[]byte("This is a Test"),
		"test",
	))
	require.NoError(t, kinesis.PutRecord(
		source,
		[]byte("testing the test!"),
		"test",
	))

	require.NoError(t, helpers.WaitDestinationEqualRowsCount(
		databaseName,
		source.Stream,
		helpers.GetSampleableStorageByModel(t, target),
		60*time.Second,
		3,
	))
	reference.Dump(t, &model.ChSource{
		Database: "public",
		ShardsList: []model.ClickHouseShard{
			{
				Name:  "_",
				Hosts: []string{"localhost"},
			},
		},
		NativePort: target.NativePort,
		HTTPPort:   target.HTTPPort,
		User:       target.User,
	})
}

func TestSnapshot(t *testing.T) {
	TestReplication(t)
}
