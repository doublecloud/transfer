package jsonlogs

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers"
	jsonparser "github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers/registry/json"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/postgres"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/yds"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/runtime/local"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers/lbenv"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

var (
	transferID = "e2e_test"
)

func TestPushClientLogs(t *testing.T) {
	lbEnv, stop := lbenv.NewLbEnv(t)

	lbPort := lbEnv.ProducerOptions().Port
	sourcePort := lbEnv.Port

	defer stop()

	loggerLbWriter, err := logger.NewLogbrokerLoggerFromConfig(&logger.LogbrokerConfig{
		Instance:    lbEnv.ProducerOptions().Endpoint,
		Port:        lbPort,
		Topic:       lbEnv.DefaultTopic,
		SourceID:    "test",
		Credentials: lbEnv.ProducerOptions().Credentials,
	}, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	parserConfigStruct := &jsonparser.ParserConfigJSONCommon{
		Fields: []abstract.ColSchema{
			{ColumnName: "ts", DataType: ytschema.TypeBytes.String()},
			{ColumnName: "level", DataType: ytschema.TypeBytes.String()},
			{ColumnName: "caller", DataType: ytschema.TypeBytes.String()},
			{ColumnName: "msg", DataType: ytschema.TypeBytes.String()},
		},
		AddRest:       false,
		AddDedupeKeys: true,
	}
	parserConfigMap, err := parsers.ParserConfigStructToMap(parserConfigStruct)
	require.NoError(t, err)

	src := &yds.YDSSource{
		Endpoint:       lbEnv.Endpoint,
		Port:           sourcePort,
		Database:       "",
		Stream:         lbEnv.DefaultTopic,
		Consumer:       lbEnv.DefaultConsumer,
		Credentials:    lbEnv.Creds,
		S3BackupBucket: "",
		BackupMode:     "",
		Transformer:    nil,
		ParserConfig:   parserConfigMap,
	}
	dstPort, _ := strconv.Atoi(os.Getenv("PG_LOCAL_PORT"))
	dst := &postgres.PgDestination{
		Hosts:     []string{"localhost"},
		ClusterID: os.Getenv("TARGET_CLUSTER_ID"),
		User:      os.Getenv("PG_LOCAL_USER"),
		Password:  server.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:  os.Getenv("PG_LOCAL_DATABASE"),
		Port:      dstPort,
	}

	transfer := &server.Transfer{
		ID:  transferID,
		Src: src,
		Dst: dst,
	}
	helpers.InitSrcDst(transferID, src, dst, abstract.TransferTypeIncrementOnly) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
	// SEND TO LOGBROKER
	go func() {
		for i := 0; i < 50; i++ {
			loggerLbWriter.Infof(fmt.Sprintf(`{"ID": "--%d--", "Bytes": %d}`, i, i))
		}
	}()
	w := local.NewLocalWorker(coordinator.NewFakeClient(), transfer, solomon.NewRegistry(solomon.NewRegistryOpts()), logger.LoggerWithLevel(zapcore.DebugLevel))
	w.Start()
	require.NoError(t, helpers.WaitDestinationEqualRowsCount("public", lbEnv.DefaultTopic, helpers.GetSampleableStorageByModel(t, dst), 60*time.Second, 50))
}
