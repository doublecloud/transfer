package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/doublecloud/tross/library/go/core/metrics/solomon"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	cpclient "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/parsers"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/parsers/registry/blank"
	"github.com/stretchr/testify/require"
)

func TestTopicResolver(t *testing.T) {
	kafkaSource, err := SourceRecipe()
	require.NoError(t, err)

	parserConfigMap, err := parsers.ParserConfigStructToMap(new(blank.ParserConfigBlankLb))
	require.NoError(t, err)
	kafkaSource.ParserConfig = parserConfigMap

	require.NoError(t, CreateSourceTopicIfNotExist(kafkaSource, "topic1", logger.Log))
	loadData(t, kafkaSource, "topic1")
	require.NoError(t, CreateSourceTopicIfNotExist(kafkaSource, "topic2", logger.Log))
	require.NoError(t, CreateSourceTopicIfNotExist(kafkaSource, "topic3", logger.Log))
	loadData(t, kafkaSource, "topic3")
	require.NoError(t, CreateSourceTopicIfNotExist(kafkaSource, "topic_ZSTD", logger.Log))
	require.NoError(t, CreateSourceTopicIfNotExist(kafkaSource, "topic_LZ4", logger.Log))
	require.NoError(t, CreateSourceTopicIfNotExist(kafkaSource, "topic_SNAPPY", logger.Log))
	require.NoError(t, CreateSourceTopicIfNotExist(kafkaSource, "topic_GZIP", logger.Log))
	loadData(t, kafkaSource, "topic_GZIP")

	time.Sleep(10 * time.Second) // just in case, to ensure our logger actually flush stuff

	provider := New(
		logger.Log,
		solomon.NewRegistry(solomon.NewRegistryOpts()),
		cpclient.NewFakeClient(),
		&server.Transfer{Src: kafkaSource, ID: "asd"},
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
