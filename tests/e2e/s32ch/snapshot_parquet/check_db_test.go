package mssql

import (
	"os"
	"testing"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/pkg/abstract"
	dp_model "github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/providers/clickhouse/model"
	"github.com/doublecloud/transfer/pkg/providers/s3"
	"github.com/doublecloud/transfer/tests/helpers"
)

var Target = model.ChDestination{
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
	Database:            "taxi",
	HTTPPort:            helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT"),
	NativePort:          helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT"),
	ProtocolUnspecified: true,
	ChClusterName:       "test_shard_localhost",
	Cleanup:             dp_model.Truncate,
}

func TestNativeS3(t *testing.T) {
	testCasePath := "fhv_taxi"
	src := s3.PrepareCfg(t, "", "")
	src.PathPrefix = testCasePath
	if os.Getenv("S3MDS_PORT") != "" { // for local recipe we need to upload test case to internet
		src.Bucket = "data3"
		s3.CreateBucket(t, src)
		s3.PrepareTestCase(t, src, src.PathPrefix)
		logger.Log.Info("dir uploaded")
	}
	src.TableNamespace = "taxi"
	src.TableName = "trip"
	Target.WithDefaults()
	transfer := helpers.MakeTransfer("fake", src, &Target, abstract.TransferTypeSnapshotOnly)
	helpers.Activate(t, transfer)
	helpers.CheckRowsCount(t, &Target, "taxi", "trip", 2439039)
}
