package sqltransformer

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	pgcommon "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/postgres"
	ytcommon "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/yt"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	yt_helpers "github.com/doublecloud/transfer/transfer_manager/go/tests/helpers/yt"
	"github.com/stretchr/testify/require"
)

var (
	srcPort = helpers.GetIntFromEnv("PG_LOCAL_PORT")
	Source  = pgcommon.PgSource{
		ClusterID: os.Getenv("PG_CLUSTER_ID"),
		Hosts:     []string{"localhost"},
		User:      os.Getenv("PG_LOCAL_USER"),
		Password:  server.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:  os.Getenv("PG_LOCAL_DATABASE"),
		Port:      srcPort,
		DBTables:  []string{"public.__test"},
	}
	Target = yt_helpers.RecipeYtTarget("//home/cdc/test/pg2yt_e2e")
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
}

func TestMain(m *testing.M) {
	ytcommon.InitExe()
	os.Exit(m.Run())
}

func TestGroup(t *testing.T) {
	targetPort, err := helpers.GetPortFromStr(Target.Cluster())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Load", Load)
	})
}

func Load(t *testing.T) {
	_ = os.Setenv("CH_LOCAL_PATH", os.Getenv("RECIPE_CLICKHOUSE_BIN"))

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, Target, abstract.TransferTypeSnapshotOnly)
	require.NoError(t, transfer.TransformationFromJSON(`
  {
    "transformers":  [
      {
        "sql":  {
          "tables":  {
            "includeTables":  [
              "^public.__test$"
            ]
          },
          "query":  "SELECT\r\n    id,\r\n    parseDateTime32BestEffort( JSONExtractString(data, 'eventTime')) AS eventTime,\r\n    JSONExtractString(data, 'sourceIPAddress') AS sourceIPAddress,\r\n    JSONExtractString(data, 'userAgent') AS userAgent,\r\n    JSONExtractString(data, 'requestParameters.bucketName') AS bucketName,\r\n    JSONExtractString(data, 'additionalEventData.SignatureVersion') AS signatureVersion,\r\n    JSONExtractString(data, 'additionalEventData.CipherSuite') AS cipherSuite,\r\n    JSONExtractUInt(data, 'additionalEventData.bytesTransferredIn') AS bytesTransferredIn,\r\n    JSONExtractString(data, 'additionalEventData.AuthenticationMethod') AS authenticationMethod,\r\n    JSONExtractUInt(data, 'additionalEventData.bytesTransferredOut') AS bytesTransferredOut,\r\n    JSONExtractString(data, 'requestID') AS requestID,\r\n    JSONExtractString(data, 'eventID') AS eventID,\r\n    JSONExtractBool(data, 'readOnly') AS readOnly,\r\n    JSONExtractString(data, 'resources[1].ARN') AS resourceARN,\r\n    JSONExtractString(data, 'eventType') AS eventType,\r\n    JSONExtractBool(data, 'managementEvent') AS managementEvent\r\nFROM table;\r\n"
        }
      }
    ]
  }
`))
	worker := helpers.Activate(t, transfer)
	require.NotNil(t, worker, "Transfer is not activated")

	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "__test",
		helpers.GetSampleableStorageByModel(t, Source),
		helpers.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second))

	_, err := pgcommon.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)

	worker.Close(t)

	storage := helpers.GetSampleableStorageByModel(t, Target.LegacyModel())
	require.NoError(t, storage.LoadTable(context.Background(), abstract.TableDescription{
		Name:   "__test",
		Schema: "",
		Filter: "",
		EtaRow: 0,
		Offset: 0,
	}, func(items []abstract.ChangeItem) error {
		for _, row := range items {
			if !row.IsRowEvent() {
				continue
			}
			require.Len(t, row.TableSchema.Columns(), 16)
			require.Equal(
				t,
				[]string{"id", "eventTime", "sourceIPAddress", "userAgent", "bucketName", "signatureVersion", "cipherSuite", "bytesTransferredIn", "authenticationMethod", "bytesTransferredOut", "requestID", "eventID", "readOnly", "resourceARN", "eventType", "managementEvent"},
				row.ColumnNames,
			)
		}
		return nil
	}))
}
