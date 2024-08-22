package datetimehack

import (
	"context"
	"testing"
	"time"

	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers"
	jsonparser "github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers/registry/json"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/logbroker"
	yt2 "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/yt"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/runtime/local"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers/lbenv"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/library/go/core/log"
	ytschema "go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

const (
	ytPath = "//home/cdc/test/logs_e2e_datetimehack_versioned_table"
)

func TestDatetimeHackDoesNotRuinVersionedTableMigrations(t *testing.T) {
	cfg := &yt.Config{}
	ytProxy, err := cfg.GetProxy()
	require.NoError(t, err)

	ytEnv, cancel := yttest.NewEnv(t)
	lbEnv, stop := lbenv.NewLbEnv(t)

	sourcePort := lbEnv.ConsumerOptions().Port
	loggerPort := lbEnv.ProducerOptions().Port
	targetPort, err := helpers.GetPortFromStr(ytProxy)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "LF source", Port: sourcePort},
			helpers.LabeledPort{Label: "Logger LB writer", Port: loggerPort},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()
	defer cancel()
	defer stop()

	lgr, err := logger.NewLogbrokerLoggerFromConfig(&logger.LogbrokerConfig{
		Instance:    lbEnv.ProducerOptions().Endpoint,
		Port:        loggerPort,
		Topic:       lbEnv.DefaultTopic,
		SourceID:    "test",
		Credentials: lbEnv.ProducerOptions().Credentials,
	}, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	parserConfigStruct := &jsonparser.ParserConfigJSONLb{
		Fields: []abstract.ColSchema{
			{ColumnName: "id", DataType: ytschema.TypeInt64.String(), PrimaryKey: true, Required: false},
			{ColumnName: "name", DataType: ytschema.TypeString.String(), PrimaryKey: false, Required: false},
			{ColumnName: "version", DataType: ytschema.TypeInt64.String(), PrimaryKey: false, Required: true},
		},
		AddRest: true,
	}
	parserConfigMap, err := parsers.ParserConfigStructToMap(parserConfigStruct)
	require.NoError(t, err)

	src := &logbroker.LfSource{
		Instance:     logbroker.LogbrokerInstance(lbEnv.Endpoint),
		Topics:       []string{lbEnv.DefaultTopic},
		Credentials:  lbEnv.ConsumerOptions().Credentials,
		Consumer:     lbEnv.DefaultConsumer,
		Port:         sourcePort,
		ParserConfig: parserConfigMap,
	}
	dst := yt2.NewYtDestinationV1(yt2.YtDestination{
		Path:          ytPath,
		Cluster:       ytProxy,
		Token:         cfg.GetToken(),
		CellBundle:    "default",
		PrimaryMedium: "default",
		PushWal:       false,
		NeedArchive:   false,
		CanAlter:      true,
		VersionColumn: "version",
		Atomicity:     yt.AtomicityFull,
	})
	dst.WithDefaults()
	transfer := &server.Transfer{
		ID:  "dttdatetimehackversionedtbl",
		Src: src,
		Dst: dst,
	}
	go func() {
		for i := int64(0); i < 50; i++ {
			lgr.Info("Bulls on parade!", log.Int64("id", i), log.Any("name", i), log.Int64("version", i))
		}
	}()
	localWorker := local.NewLocalWorker(coordinator.NewFakeClient(), transfer, solomon.NewRegistry(solomon.NewRegistryOpts()), logger.Log)
	localWorker.Start()
	defer func() {
		err := localWorker.Stop()
		require.NoError(t, ytEnv.YT.RemoveNode(context.TODO(), ypath.Path(ytPath), &yt.RemoveNodeOptions{
			Recursive: true,
			Force:     true,
		}))
		require.NoError(t, err)
	}()
	require.NoError(t, helpers.WaitDestinationEqualRowsCount("", lbEnv.DefaultTopic, helpers.GetSampleableStorageByModel(t, dst.LegacyModel()), 60*time.Second, 50))
}
