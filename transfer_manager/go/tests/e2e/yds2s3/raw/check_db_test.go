package main

import (
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	s3_provider "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/s3"
	_ "github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/s3/provider"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/yds"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/runtime/local"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers/lbenv"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
	"go.ytsaurus.tech/library/go/core/log"
)

var (
	testBucket    = envOrDefault("TEST_BUCKET", "barrel")
	testAccessKey = envOrDefault("TEST_ACCESS_KEY_ID", "1234567890")
	testSecret    = envOrDefault("TEST_SECRET_ACCESS_KEY", "abcdefabcdef")
)

func envOrDefault(key string, def string) string {
	if os.Getenv(key) != "" {
		return os.Getenv(key)
	}
	return def
}

func createBucket(t *testing.T, cfg *s3_provider.S3Destination) {
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(cfg.Endpoint),
		Region:           aws.String(cfg.Region),
		S3ForcePathStyle: aws.Bool(cfg.S3ForcePathStyle),
		Credentials: credentials.NewStaticCredentials(
			cfg.AccessKey, cfg.Secret, "",
		),
	})
	require.NoError(t, err)
	res, err := s3.New(sess).CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(cfg.Bucket),
	})
	require.NoError(t, err)
	logger.Log.Info("create bucket result", log.Any("res", res))
}

func TestReplication(t *testing.T) {
	lbEnv, stop := lbenv.NewLbEnv(t)
	defer stop()
	lbSendingPort := lbEnv.ProducerOptions().Port
	lbReceivingPort := lbEnv.Port

	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "PG source", Port: lbSendingPort},
	))

	loggerLbWriter, err := logger.NewLogbrokerLoggerFromConfig(&logger.LogbrokerConfig{
		Instance:    lbEnv.ProducerOptions().Endpoint,
		Port:        lbSendingPort,
		Topic:       lbEnv.DefaultTopic,
		SourceID:    "test",
		Credentials: lbEnv.ProducerOptions().Credentials,
	}, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	src := yds.YDSSource{
		Endpoint:       lbEnv.Endpoint,
		Port:           lbReceivingPort,
		Database:       "",
		Stream:         lbEnv.DefaultTopic,
		Consumer:       lbEnv.DefaultConsumer,
		Credentials:    lbEnv.Creds,
		S3BackupBucket: "",
		BackupMode:     "",
		Transformer:    nil,
		ParserConfig:   nil,
	}

	dst := &s3_provider.S3Destination{
		OutputFormat:     server.ParsingFormatRaw,
		BufferSize:       1 * 1024 * 1024,
		BufferInterval:   time.Second * 5,
		Bucket:           testBucket,
		AccessKey:        testAccessKey,
		S3ForcePathStyle: true,
		Secret:           testSecret,
		Layout:           "test",
		Region:           "eu-central1",
	}
	dst.WithDefaults()

	if os.Getenv("S3MDS_PORT") != "" {
		dst.Endpoint = fmt.Sprintf("http://localhost:%v", os.Getenv("S3MDS_PORT"))
		createBucket(t, dst)
	}

	// activate transfer
	helpers.InitSrcDst(helpers.TransferID, &src, dst, abstract.TransferTypeIncrementOnly)
	transfer := helpers.MakeTransfer(helpers.TransferID, &src, dst, abstract.TransferTypeIncrementOnly)

	for i := 0; i < 1000; i++ {
		loggerLbWriter.Infof("blablabla: %d", i)
	}

	localWorker := local.NewLocalWorker(coordinator.NewFakeClient(), transfer, solomon.NewRegistry(solomon.NewRegistryOpts()), logger.LoggerWithLevel(zapcore.DebugLevel))
	localWorker.Start()
	defer localWorker.Stop()

	//-----------------------------------------------------------------------------------------------------------------
	// send to logbroker

	time.Sleep(time.Second * 10)
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(dst.Endpoint),
		Region:           aws.String(dst.Region),
		S3ForcePathStyle: aws.Bool(dst.S3ForcePathStyle),
		Credentials: credentials.NewStaticCredentials(
			dst.AccessKey, dst.Secret, "",
		),
	})

	require.NoError(t, err)
	s3client := s3.New(sess)
	objects, err := s3client.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(dst.Bucket),
	})
	require.NoError(t, err)
	logger.Log.Infof("objects: %v", objects.Contents)
	require.Len(t, objects.Contents, 1)
	obj, err := s3client.GetObject(&s3.GetObjectInput{Bucket: aws.String(dst.Bucket), Key: objects.Contents[0].Key})
	require.NoError(t, err)
	data, err := io.ReadAll(obj.Body)
	require.NoError(t, err)
	logger.Log.Infof("read file: %s /n%s", *objects.Contents[0].Key, string(data))
	require.True(t, strings.HasSuffix(*objects.Contents[0].Key, "0_999.raw"))
}
