package kinesis

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract/coordinator"
	"github.com/doublecloud/transfer/pkg/parsequeue"
	"github.com/doublecloud/transfer/pkg/providers/kinesis/consumer"
	"github.com/doublecloud/transfer/pkg/stats"
	"github.com/stretchr/testify/require"
	"testing"
)

type fakeClient struct {
}

func (f fakeClient) ListShards(input *kinesis.ListShardsInput) (*kinesis.ListShardsOutput, error) {
	return &kinesis.ListShardsOutput{Shards: []*kinesis.Shard{
		{ShardId: aws.String("s1")},
		{ShardId: aws.String("s2")},
		{ShardId: aws.String("s3")},
	}}, nil
}

func (f fakeClient) GetRecords(input *kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error) {
	return nil, awserr.New("non-retryable-code", "asd", xerrors.New("demo error"))
}

func (f fakeClient) GetShardIteratorWithContext(a aws.Context, input *kinesis.GetShardIteratorInput, option ...request.Option) (*kinesis.GetShardIteratorOutput, error) {
	return &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("s1")}, nil
}

func TestFailure(t *testing.T) {
	var err error
	s := new(Source)
	s.cp = coordinator.NewFakeClient()
	s.logger = logger.Log
	s.ctx = context.Background()
	s.config = new(KinesisSource)
	s.config.WithDefaults()
	s.metrics = stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts()))
	s.consumer, err = consumer.New("abc", consumer.WithClient(&fakeClient{}))
	require.NoError(t, err)
	parseQ := parsequeue.NewWaitable(s.logger, 10, nil, s.parse, s.ack)
	require.Error(t, s.run(parseQ))
}
