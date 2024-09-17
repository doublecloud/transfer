package kafka

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/pkg/abstract"
	server "github.com/doublecloud/transfer/pkg/abstract/model"
	debeziumparameters "github.com/doublecloud/transfer/pkg/debezium/parameters"
	gomock "github.com/golang/mock/gomock"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

var sinkTestTypicalChangeItem *abstract.ChangeItem
var sinkTestMirrorChangeItem *abstract.ChangeItem

func init() {
	var testChangeItem = `{"id":601,"nextlsn":25051056,"commitTime":1643660670333075000,"txPosition":0,"kind":"insert","schema":"public","table":"basic_types15","columnnames":["id","val"],"columnvalues":[1,-8388605],"table_schema":[{"path":"","name":"id","type":"int32","key":true,"required":false,"original_type":"pg:integer","original_type_params":null},{"path":"","name":"val","type":"int32","key":false,"required":false,"original_type":"pg:integer","original_type_params":null}],"oldkeys":{},"tx_id":"","query":""}`
	sinkTestTypicalChangeItem, _ = abstract.UnmarshalChangeItem([]byte(testChangeItem))
	var testMirrorChangeItem = `{"id":0,"nextlsn":49,"commitTime":1648053051911000000,"txPosition":0,"kind":"insert","schema":"default-topic","table":"94","columnnames":["topic","partition","seq_no","write_time","data"],"columnvalues":["default-topic",94,50,"2022-03-23T19:30:51.911+03:00","blablabla"],"table_schema":[{"path":"","name":"topic","type":"utf8","key":true,"required":false,"original_type":"","original_type_params":null},{"path":"","name":"partition","type":"uint32","key":true,"required":false,"original_type":"","original_type_params":null},{"path":"","name":"seq_no","type":"uint64","key":true,"required":false,"original_type":"","original_type_params":null},{"path":"","name":"write_time","type":"datetime","key":true,"required":false,"original_type":"","original_type_params":null},{"path":"","name":"data","type":"utf8","key":false,"required":false,"original_type":"mirror:binary","original_type_params":null}],"oldkeys":{},"tx_id":"","query":""}`
	sinkTestMirrorChangeItem, _ = abstract.UnmarshalChangeItem([]byte(testMirrorChangeItem))
}

func TestNative(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dst := &KafkaDestination{
		Connection: &KafkaConnectionOptions{
			TLS:     server.DefaultTLS,
			Brokers: []string{"my_broker_0"},
		},
		Auth: &KafkaAuth{
			Enabled:   true,
			Mechanism: "SHA-512",
			User:      "user1",
			Password:  "qwert12345",
		},
		Topic: "foo_bar",
		FormatSettings: server.SerializationFormat{
			Name: server.SerializationFormatNative,
		},
	}
	dst.WithDefaults()

	value, err := json.Marshal(sinkTestTypicalChangeItem)
	require.NoError(t, err)

	writer := NewMockwriter(ctrl)
	writer.EXPECT().WriteMessages(gomock.Any(), []kafka.Message{{Key: []byte("public_basic_types15"), Value: value, Topic: "foo_bar"}}).Return(nil)
	client := NewMockclient(ctrl)
	client.EXPECT().BuildWriter([]string{"my_broker_0"}, gomock.Any(), gomock.Any(), gomock.Any()).Return(writer)
	client.EXPECT().CreateTopicIfNotExist([]string{"my_broker_0"}, "foo_bar", gomock.Any(), gomock.Any(), gomock.Any())

	testSink, err := NewSinkImpl(
		dst,
		solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()}),
		logger.Log,
		client,
		false,
	)
	require.NoError(t, err)

	err = testSink.Push([]abstract.ChangeItem{*sinkTestTypicalChangeItem})
	require.NoError(t, err)
}

func TestJSON(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dst := &KafkaDestination{
		Connection: &KafkaConnectionOptions{
			TLS:     server.DefaultTLS,
			Brokers: []string{"my_broker_0"},
		},
		Auth: &KafkaAuth{
			Enabled:   true,
			Mechanism: "SHA-512",
			User:      "user1",
			Password:  "qwert12345",
		},
		Topic: "foo_bar",
		FormatSettings: server.SerializationFormat{
			Name: server.SerializationFormatJSON,
		},
	}
	dst.WithDefaults()

	k := `public_basic_types15`
	v := `{"id":1,"val":-8388605}`

	writer := NewMockwriter(ctrl)
	writer.EXPECT().WriteMessages(gomock.Any(), []kafka.Message{{Key: []byte(k), Value: []byte(v), Topic: "foo_bar"}}).Return(nil)
	client := NewMockclient(ctrl)
	client.EXPECT().BuildWriter([]string{"my_broker_0"}, gomock.Any(), gomock.Any(), gomock.Any()).Return(writer)
	client.EXPECT().CreateTopicIfNotExist([]string{"my_broker_0"}, "foo_bar", gomock.Any(), gomock.Any(), gomock.Any())

	testSink, err := NewSinkImpl(
		dst,
		solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()}),
		logger.Log,
		client,
		false,
	)
	require.NoError(t, err)

	err = testSink.Push([]abstract.ChangeItem{*sinkTestTypicalChangeItem})
	require.NoError(t, err)
}

func TestDebezium(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dst := &KafkaDestination{
		Connection: &KafkaConnectionOptions{
			TLS:     server.DefaultTLS,
			Brokers: []string{"my_broker_0"},
		},
		Auth: &KafkaAuth{
			Enabled:   true,
			Mechanism: "SHA-512",
			User:      "user1",
			Password:  "qwert12345",
		},
		TopicPrefix: "foo_bar",
		FormatSettings: server.SerializationFormat{
			Name: server.SerializationFormatDebezium,
			Settings: map[string]string{
				debeziumparameters.DatabaseDBName: "",
				debeziumparameters.SourceType:     "pg",
			},
		},
	}
	dst.WithDefaults()

	k := `{"payload":{"id":1},"schema":{"fields":[{"field":"id","optional":false,"type":"int32"}],"name":"foo_bar.public.basic_types15.Key","optional":false,"type":"struct"}}`
	v := `{"payload":{"after":{"id":1,"val":-8388605},"before":null,"op":"c","source":{"connector":"postgresql","db":"","lsn":25051056,"name":"foo_bar","schema":"public","snapshot":"false","table":"basic_types15","ts_ms":1643660670333,"txId":601,"version":"1.1.2.Final","xmin":null},"transaction":null,"ts_ms":1643660670333},"schema":{"fields":[{"field":"before","fields":[{"field":"id","optional":false,"type":"int32"},{"field":"val","optional":true,"type":"int32"}],"name":"foo_bar.public.basic_types15.Value","optional":true,"type":"struct"},{"field":"after","fields":[{"field":"id","optional":false,"type":"int32"},{"field":"val","optional":true,"type":"int32"}],"name":"foo_bar.public.basic_types15.Value","optional":true,"type":"struct"},{"field":"source","fields":[{"field":"version","optional":false,"type":"string"},{"field":"connector","optional":false,"type":"string"},{"field":"name","optional":false,"type":"string"},{"field":"ts_ms","optional":false,"type":"int64"},{"default":"false","field":"snapshot","name":"io.debezium.data.Enum","optional":true,"parameters":{"allowed":"true,last,false"},"type":"string","version":1},{"field":"db","optional":false,"type":"string"},{"field":"table","optional":false,"type":"string"},{"field":"lsn","optional":true,"type":"int64"},{"field":"schema","optional":false,"type":"string"},{"field":"txId","optional":true,"type":"int64"},{"field":"xmin","optional":true,"type":"int64"}],"name":"io.debezium.connector.postgresql.Source","optional":false,"type":"struct"},{"field":"op","optional":false,"type":"string"},{"field":"ts_ms","optional":true,"type":"int64"},{"field":"transaction","fields":[{"field":"id","optional":false,"type":"string"},{"field":"total_order","optional":false,"type":"int64"},{"field":"data_collection_order","optional":false,"type":"int64"}],"optional":true,"type":"struct"}],"name":"foo_bar.public.basic_types15.Envelope","optional":false,"type":"struct"}}`

	writer := NewMockwriter(ctrl)
	writer.EXPECT().WriteMessages(gomock.Any(), []kafka.Message{{Key: []byte(k), Value: []byte(v), Topic: "foo_bar.public.basic_types15"}})
	client := NewMockclient(ctrl)
	client.EXPECT().BuildWriter([]string{"my_broker_0"}, gomock.Any(), gomock.Any(), gomock.Any()).Return(writer)
	client.EXPECT().CreateTopicIfNotExist([]string{"my_broker_0"}, "foo_bar.public.basic_types15", gomock.Any(), gomock.Any(), gomock.Any())

	testSink, err := NewSinkImpl(
		dst,
		solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()}),
		logger.Log,
		client,
		false,
	)
	require.NoError(t, err)

	err = testSink.Push([]abstract.ChangeItem{*sinkTestTypicalChangeItem})
	require.NoError(t, err)
}

func TestMirror(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dst := &KafkaDestination{
		Connection: &KafkaConnectionOptions{
			TLS:     server.DefaultTLS,
			Brokers: []string{"my_broker_0"},
		},
		Auth: &KafkaAuth{
			Enabled:   true,
			Mechanism: "SHA-512",
			User:      "user1",
			Password:  "qwert12345",
		},
		Topic: "foo_bar",
		FormatSettings: server.SerializationFormat{
			Name: server.SerializationFormatMirror,
		},
	}
	dst.WithDefaults()

	v := `blablabla`

	writer := NewMockwriter(ctrl)
	writer.EXPECT().WriteMessages(gomock.Any(), []kafka.Message{{Value: []byte(v), Topic: "foo_bar"}})
	client := NewMockclient(ctrl)
	client.EXPECT().BuildWriter([]string{"my_broker_0"}, gomock.Any(), gomock.Any(), gomock.Any()).Return(writer)
	client.EXPECT().CreateTopicIfNotExist([]string{"my_broker_0"}, "foo_bar", gomock.Any(), gomock.Any(), gomock.Any())

	testSink, err := NewSinkImpl(
		dst,
		solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()}),
		logger.Log,
		client,
		false,
	)
	require.NoError(t, err)

	err = testSink.Push([]abstract.ChangeItem{*sinkTestMirrorChangeItem})
	require.NoError(t, err)
}

func TestMirrorKafka(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dst := &KafkaDestination{
		Connection: &KafkaConnectionOptions{
			TLS:     server.DefaultTLS,
			Brokers: []string{"my_broker_0"},
		},
		Auth: &KafkaAuth{
			Enabled:   true,
			Mechanism: "SHA-512",
			User:      "user1",
			Password:  "qwert12345",
		},
		Topic: "foo_bar",
		FormatSettings: server.SerializationFormat{
			Name: server.SerializationFormatMirror,
		},
	}
	dst.WithDefaults()

	k := []byte(`my_key`)
	v := []byte(`blablabla`)

	writer := NewMockwriter(ctrl)
	writer.EXPECT().WriteMessages(gomock.Any(), []kafka.Message{{Key: k, Value: v, Topic: "foo_bar"}})
	client := NewMockclient(ctrl)
	client.EXPECT().BuildWriter([]string{"my_broker_0"}, gomock.Any(), gomock.Any(), gomock.Any()).Return(writer)
	client.EXPECT().CreateTopicIfNotExist([]string{"my_broker_0"}, "foo_bar", gomock.Any(), gomock.Any(), gomock.Any())

	testSink, err := NewSinkImpl(
		dst,
		solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()}),
		logger.Log,
		client,
		false,
	)
	require.NoError(t, err)

	err = testSink.Push([]abstract.ChangeItem{MakeKafkaRawMessage("foo_bar", time.Time{}, "foo_bar", 0, 0, k, v)})
	require.NoError(t, err)
}

func TestAddDTSystemTables(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dst := &KafkaDestination{
		Connection: &KafkaConnectionOptions{
			TLS:     server.DefaultTLS,
			Brokers: []string{"my_broker_0"},
		},
		Auth: &KafkaAuth{
			Enabled:   true,
			Mechanism: "SHA-512",
			User:      "user1",
			Password:  "qwert12345",
		},
		TopicPrefix: "foo_bar",
		FormatSettings: server.SerializationFormat{
			Name: server.SerializationFormatDebezium,
			Settings: map[string]string{
				debeziumparameters.SourceType: "pg",
			},
		},
	}
	dst.WithDefaults()

	currChangeItem := *sinkTestTypicalChangeItem
	currChangeItem.Table = abstract.TableConsumerKeeper

	t.Run("false", func(t *testing.T) {
		dst1 := dst
		dst1.AddSystemTables = false

		writer := NewMockwriter(ctrl)
		client := NewMockclient(ctrl)
		client.EXPECT().BuildWriter([]string{"my_broker_0"}, gomock.Any(), gomock.Any(), gomock.Any()).Return(writer)

		testSink, err := NewSinkImpl(
			dst1,
			solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()}),
			logger.Log,
			client,
			false,
		)
		require.NoError(t, err)

		err = testSink.Push([]abstract.ChangeItem{currChangeItem})
		require.NoError(t, err)
	})

	t.Run("true", func(t *testing.T) {
		dst2 := dst
		dst2.AddSystemTables = true

		k := `{"payload":{"id":1},"schema":{"fields":[{"field":"id","optional":false,"type":"int32"}],"name":"foo_bar.public.__consumer_keeper.Key","optional":false,"type":"struct"}}`
		v := `{"payload":{"after":{"id":1,"val":-8388605},"before":null,"op":"c","source":{"connector":"postgresql","db":"","lsn":25051056,"name":"foo_bar","schema":"public","snapshot":"false","table":"__consumer_keeper","ts_ms":1643660670333,"txId":601,"version":"1.1.2.Final","xmin":null},"transaction":null,"ts_ms":1643660670333},"schema":{"fields":[{"field":"before","fields":[{"field":"id","optional":false,"type":"int32"},{"field":"val","optional":true,"type":"int32"}],"name":"foo_bar.public.__consumer_keeper.Value","optional":true,"type":"struct"},{"field":"after","fields":[{"field":"id","optional":false,"type":"int32"},{"field":"val","optional":true,"type":"int32"}],"name":"foo_bar.public.__consumer_keeper.Value","optional":true,"type":"struct"},{"field":"source","fields":[{"field":"version","optional":false,"type":"string"},{"field":"connector","optional":false,"type":"string"},{"field":"name","optional":false,"type":"string"},{"field":"ts_ms","optional":false,"type":"int64"},{"default":"false","field":"snapshot","name":"io.debezium.data.Enum","optional":true,"parameters":{"allowed":"true,last,false"},"type":"string","version":1},{"field":"db","optional":false,"type":"string"},{"field":"table","optional":false,"type":"string"},{"field":"lsn","optional":true,"type":"int64"},{"field":"schema","optional":false,"type":"string"},{"field":"txId","optional":true,"type":"int64"},{"field":"xmin","optional":true,"type":"int64"}],"name":"io.debezium.connector.postgresql.Source","optional":false,"type":"struct"},{"field":"op","optional":false,"type":"string"},{"field":"ts_ms","optional":true,"type":"int64"},{"field":"transaction","fields":[{"field":"id","optional":false,"type":"string"},{"field":"total_order","optional":false,"type":"int64"},{"field":"data_collection_order","optional":false,"type":"int64"}],"optional":true,"type":"struct"}],"name":"foo_bar.public.__consumer_keeper.Envelope","optional":false,"type":"struct"}}`

		topicName := fmt.Sprintf("foo_bar.public.%s", abstract.TableConsumerKeeper)

		writer := NewMockwriter(ctrl)
		writer.EXPECT().WriteMessages(gomock.Any(), []kafka.Message{{Key: []byte(k), Value: []byte(v), Topic: topicName}})
		client := NewMockclient(ctrl)
		client.EXPECT().BuildWriter([]string{"my_broker_0"}, gomock.Any(), gomock.Any(), gomock.Any()).Return(writer)
		client.EXPECT().CreateTopicIfNotExist([]string{"my_broker_0"}, topicName, gomock.Any(), gomock.Any(), gomock.Any())

		testSink, err := NewSinkImpl(
			dst2,
			solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()}),
			logger.Log,
			client,
			false,
		)
		require.NoError(t, err)

		err = testSink.Push([]abstract.ChangeItem{currChangeItem})
		require.NoError(t, err)
	})
}

func TestPassConfigEntries(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dst := &KafkaDestination{
		Connection: &KafkaConnectionOptions{
			TLS:     server.DefaultTLS,
			Brokers: []string{"my_broker_0"},
		},
		Auth: &KafkaAuth{
			Enabled:   true,
			Mechanism: "SHA-512",
			User:      "user1",
			Password:  "qwert12345",
		},
		Topic: "foo_bar",
		FormatSettings: server.SerializationFormat{
			Name: server.SerializationFormatJSON,
		},
		TopicConfigEntries: []TopicConfigEntry{
			{
				ConfigName:  "cleanup.policy",
				ConfigValue: "compact",
			},
			{
				ConfigName:  "segment.bytes",
				ConfigValue: "200",
			},
		},
	}
	dst.WithDefaults()

	k := `public_basic_types15`
	v := `{"id":1,"val":-8388605}`

	writer := NewMockwriter(ctrl)
	writer.EXPECT().WriteMessages(gomock.Any(), []kafka.Message{{Key: []byte(k), Value: []byte(v), Topic: "foo_bar"}}).Return(nil)
	client := NewMockclient(ctrl)
	client.EXPECT().BuildWriter([]string{"my_broker_0"}, gomock.Any(), gomock.Any(), gomock.Any()).Return(writer)
	client.EXPECT().CreateTopicIfNotExist(
		[]string{"my_broker_0"},
		"foo_bar",
		gomock.Any(),
		gomock.Any(),
		[]TopicConfigEntry{
			{
				ConfigName:  "cleanup.policy",
				ConfigValue: "compact",
			},
			{
				ConfigName:  "segment.bytes",
				ConfigValue: "200",
			},
		},
	)

	testSink, err := NewSinkImpl(
		dst,
		solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()}),
		logger.Log,
		client,
		false,
	)
	require.NoError(t, err)

	err = testSink.Push([]abstract.ChangeItem{*sinkTestTypicalChangeItem})
	require.NoError(t, err)
}
