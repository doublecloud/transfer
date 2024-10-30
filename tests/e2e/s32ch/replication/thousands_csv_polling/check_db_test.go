package polling

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/doublecloud/transfer/pkg/abstract"
	dp_model "github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/providers/clickhouse/model"
	"github.com/doublecloud/transfer/pkg/providers/s3"
	"github.com/doublecloud/transfer/tests/helpers"
	"github.com/stretchr/testify/require"
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

var dst = model.ChDestination{
	ShardsList: []model.ClickHouseShard{
		{
			Name: "_",
			Hosts: []string{
				"localhost",
			},
		},
	},
	User:                "default",
	Password:            "",
	Database:            "test",
	HTTPPort:            helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT"),
	NativePort:          helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT"),
	ProtocolUnspecified: true,
	Cleanup:             dp_model.Drop,
}

func TestNativeS3(t *testing.T) {
	testCasePath := "thousands_of_csv_files"
	src := s3.PrepareCfg(t, "data4", "")
	src.PathPrefix = testCasePath
	if os.Getenv("S3MDS_PORT") != "" { // for local recipe we need to upload test case to internet
		s3.PrepareTestCase(t, src, src.PathPrefix)
	}

	time.Sleep(5 * time.Second)

	src.TableNamespace = "test"
	src.TableName = "data"
	src.InputFormat = dp_model.ParsingFormatCSV
	src.WithDefaults()
	dst.WithDefaults()
	src.Format.CSVSetting.BlockSize = 10000000
	src.ReadBatchSize = 4000 // just for testing so its faster, normally much smaller
	src.Format.CSVSetting.QuoteChar = "\""

	start := time.Now()
	transfer := helpers.MakeTransfer("fake", src, &dst, abstract.TransferTypeIncrementOnly)
	helpers.Activate(t, transfer)

	err := helpers.WaitDestinationEqualRowsCount("test", "data", helpers.GetSampleableStorageByModel(t, transfer.Dst), 500*time.Second, 426560)
	require.NoError(t, err)
	finish := time.Now()

	duration := finish.Sub(start)
	fmt.Println("Execution took:", duration)
}
