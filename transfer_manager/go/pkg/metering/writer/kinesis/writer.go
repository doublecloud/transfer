package kinesis

import (
	"context"
	"crypto/tls"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	awsCredentials "github.com/aws/aws-sdk-go/aws/credentials"
	awsSession "github.com/aws/aws-sdk-go/aws/session"
	awsKinesis "github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/doublecloud/tross/library/go/core/log"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/config"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/metering/writer"
	"golang.org/x/exp/slices"
)

func init() {
	config.RegisterTypeTagged((*config.WriterConfig)(nil), (*Kinesis)(nil), (*Kinesis)(nil).Type(), nil)
	writer.Register(
		new(Kinesis).Type(),
		func(schema, topic, sourceID string, cfg writer.WriterConfig) (writer.Writer, error) {
			return New(cfg.(*Kinesis), schema, topic, sourceID, logger.Log)
		},
	)
}

type Kinesis struct {
	Region             string        `mapstructure:"region"`
	Endpoint           string        `mapstructure:"endpoint"`
	AccessKey          config.Secret `mapstructure:"access_key"`
	SecretKey          config.Secret `mapstructure:"secret_key"`
	InsecureSkipVerify bool          `mapstructure:"insecure_skip_verify,local_only"`
	DebugLog           bool          `mapstructure:"debug_log,local_only"`
}

func (*Kinesis) IsMeteringWriter() {}
func (*Kinesis) IsTypeTagged()     {}
func (*Kinesis) Type() string      { return "kinesis" }

type Writer struct {
	cfg    *Kinesis
	logger log.Logger
	client *awsKinesis.Kinesis
	stream string
}

func (w *Writer) Close() error {
	return nil
}

func New(config *Kinesis, schema string, topic string, id string, l log.Logger) (*Writer, error) {
	httpClient := new(http.Client)

	sdkLogs := aws.LogOff
	if config.DebugLog {
		sdkLogs = aws.LogDebugWithHTTPBody
	}
	tlsConfig := new(tls.Config)
	tlsConfig.InsecureSkipVerify = config.InsecureSkipVerify
	httpClient.Transport = &http.Transport{TLSClientConfig: tlsConfig}
	session, err := awsSession.NewSession(aws.NewConfig().
		WithRegion(config.Region).
		WithCredentials(awsCredentials.NewStaticCredentials(string(config.AccessKey), string(config.SecretKey), "")).
		WithEndpoint(config.Endpoint).
		WithHTTPClient(httpClient).
		WithLogLevel(sdkLogs).
		WithMaxRetries(0). // Forbid retries to prevent duplicates
		WithLogger(aws.LoggerFunc(func(args ...interface{}) {
			l.Infof("%+v", args...)
		})),
	)
	if err != nil {
		return nil, err
	}

	return &Writer{
		client: awsKinesis.New(session),
		cfg:    config,
		logger: l,
		stream: topic,
	}, nil
}

var (
	retriableErrorCodes = []string{
		awsKinesis.ErrCodeInternalFailureException,
		awsKinesis.ErrCodeInvalidArgumentException,
		awsKinesis.ErrCodeLimitExceededException,
		awsKinesis.ErrCodeProvisionedThroughputExceededException,
		awsKinesis.ErrCodeValidationException,
		awsKinesis.ErrCodeResourceNotFoundException,
	}
)

func (w *Writer) Write(data string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	request := &awsKinesis.PutRecordsInput{StreamName: &w.stream}
	request.Records = append(request.Records, &awsKinesis.PutRecordsRequestEntry{
		Data:         []byte(data),
		PartitionKey: aws.String("same partition"),
	})
	_, err := w.client.PutRecordsWithContext(ctx, request)
	if err == nil {
		return nil
	}
	if err, ok := err.(awserr.Error); ok {
		if slices.Contains(retriableErrorCodes, err.Code()) {
			return err
		}
	}
	w.logger.Warnf("write batch: %+v", err)
	return nil
}
