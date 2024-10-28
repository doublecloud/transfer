package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/pkg/abstract"
	cpclient "github.com/doublecloud/transfer/pkg/abstract/coordinator"
	"github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/parsers"
	"github.com/doublecloud/transfer/pkg/parsers/registry/blank"
	"github.com/doublecloud/transfer/pkg/providers/kafka/client"
	"github.com/stretchr/testify/require"
)

func TestTopicResolver(t *testing.T) {
	kafkaSource, err := SourceRecipe()
	require.NoError(t, err)

	parserConfigMap, err := parsers.ParserConfigStructToMap(new(blank.ParserConfigBlankLb))
	require.NoError(t, err)
	kafkaSource.ParserConfig = parserConfigMap

	kafkaClient, err := client.NewClient(kafkaSource.Connection.Brokers, nil, nil)
	require.NoError(t, err)
	require.NoError(t, kafkaClient.CreateTopicIfNotExist(logger.Log, "topic1", nil))
	loadData(t, kafkaSource, "topic1")
	require.NoError(t, kafkaClient.CreateTopicIfNotExist(logger.Log, "topic2", nil))
	require.NoError(t, kafkaClient.CreateTopicIfNotExist(logger.Log, "topic3", nil))
	loadData(t, kafkaSource, "topic3")
	require.NoError(t, kafkaClient.CreateTopicIfNotExist(logger.Log, "topic_ZSTD", nil))
	require.NoError(t, kafkaClient.CreateTopicIfNotExist(logger.Log, "topic_LZ4", nil))
	require.NoError(t, kafkaClient.CreateTopicIfNotExist(logger.Log, "topic_SNAPPY", nil))
	require.NoError(t, kafkaClient.CreateTopicIfNotExist(logger.Log, "topic_GZIP", nil))
	loadData(t, kafkaSource, "topic_GZIP")

	time.Sleep(10 * time.Second) // just in case, to ensure our logger actually flush stuff

	provider := New(
		logger.Log,
		solomon.NewRegistry(solomon.NewRegistryOpts()),
		cpclient.NewFakeClient(),
		&model.Transfer{Src: kafkaSource, ID: "asd"},
	).(*Provider)

	sniffer, err := provider.Sniffer(context.Background())
	require.NoError(t, err)
	items, err := sniffer.Fetch()
	require.NoError(t, err)
	topicSniff := map[string][]abstract.ChangeItem{}
	for _, row := range items {
		topicSniff[row.Table] = append(topicSniff[row.Table], row)
	}
	require.Len(t, topicSniff["{\"cluster\":\"\",\"partition\":0,\"topic\":\"topic1\"}"], 3)
	require.Len(t, topicSniff["{\"cluster\":\"\",\"partition\":0,\"topic\":\"topic3\"}"], 3)
	require.Len(t, topicSniff["{\"cluster\":\"\",\"partition\":0,\"topic\":\"topic_GZIP\"}"], 3)
	abstract.Dump(items)
}

func loadData(t *testing.T, kafkaSource *KafkaSource, topic string) {
	lgr, closer, err := logger.NewKafkaLogger(&logger.KafkaConfig{
		Broker:   kafkaSource.Connection.Brokers[0],
		Topic:    topic,
		User:     kafkaSource.Auth.User,
		Password: kafkaSource.Auth.Password,
	})
	require.NoError(t, err)

	defer closer.Close()
	for i := 0; i < 10; i++ {
		lgr.Infof("log item: %v", i)
	}
}
