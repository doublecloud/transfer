package logbroker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/recipe"
	"github.com/doublecloud/tross/library/go/core/log"
	"github.com/doublecloud/tross/library/go/core/metrics/solomon"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
	debeziumparameters "github.com/doublecloud/tross/transfer_manager/go/pkg/debezium/parameters"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/parsers/registry/blank"
	"github.com/doublecloud/tross/transfer_manager/go/tests/helpers/lbenv"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

var (
	sinkTestTypicalChangeItem  *abstract.ChangeItem
	sinkTestLbMirrorChangeItem *abstract.ChangeItem
)

func init() {
	testChangeItem := `{"id":601,"nextlsn":25051056,"commitTime":1643660670333075000,"txPosition":0,"kind":"insert","schema":"public","table":"basic_types15","columnnames":["id","val"],"columnvalues":[1,-8388605],"table_schema":[{"path":"","name":"id","type":"int32","key":true,"required":false,"original_type":"pg:integer","original_type_params":null},{"path":"","name":"val","type":"int32","key":false,"required":false,"original_type":"pg:integer","original_type_params":null}],"oldkeys":{},"tx_id":"","query":""}`
	sinkTestTypicalChangeItem, _ = abstract.UnmarshalChangeItem([]byte(testChangeItem))

	// mirrorChangeItem
	// in format of pkg/parsers/blank_parser (if format from logfeller/source.go)
	testMirrorChangeItem := `{"id":0,"nextlsn":123,"commitTime":234,"txPosition":0,"kind":"insert","schema":"raw_data","table":"my_topic_name","columnnames":["partition","offset","seq_no","source_id","c_time","w_time","ip","lb_raw_message","lb_extra_fields"],"columnvalues":["{\"cluster\":\"\",\"partition\":345,\"topic\":\"\"}",456,567,"my_source_id","2022-03-23T19:30:51.911+03:00","2022-03-24T19:30:51.911+03:00","1.1.1.1","my_data",{"my_key1":"my_val1","my_key2":"my_val2"}],"table_schema":[{"path":"","name":"partition","type":"string","key":true,"required":false,"original_type":""},{"path":"","name":"offset","type":"uint64","key":true,"required":false,"original_type":""},{"path":"","name":"seq_no","type":"uint64","key":false,"required":false,"original_type":""},{"path":"","name":"source_id","type":"string","key":false,"required":false,"original_type":""},{"path":"","name":"c_time","type":"datetime","key":false,"required":false,"original_type":""},{"path":"","name":"w_time","type":"datetime","key":false,"required":false,"original_type":""},{"path":"","name":"ip","type":"string","key":false,"required":false,"original_type":""},{"path":"","name":"lb_raw_message","type":"string","key":false,"required":false,"original_type":""},{"path":"","name":"lb_extra_fields","type":"any","key":false,"required":false,"original_type":""}],"oldkeys":{},"tx_id":"","query":""}`
	sinkTestLbMirrorChangeItem, _ = abstract.UnmarshalChangeItem([]byte(testMirrorChangeItem))
	rawDataIndex := blank.BlankColsIDX[blank.RawMessageColumn]
	rawData := sinkTestLbMirrorChangeItem.ColumnValues[rawDataIndex]
	sinkTestLbMirrorChangeItem.ColumnValues[rawDataIndex] = []byte(rawData.(string))
	extrasIndex := blank.BlankColsIDX[blank.ExtrasColumn]
	extrasMapToInterface := sinkTestLbMirrorChangeItem.ColumnValues[extrasIndex]
	extrasStr, _ := json.Marshal(extrasMapToInterface)
	var extras map[string]string
	_ = json.Unmarshal(extrasStr, &extras)
	sinkTestLbMirrorChangeItem.ColumnValues[extrasIndex] = extras
}

func NewTestWriterConfigFactory(t *testing.T) WriterConfigFactory {
	return func(config *LbDestination, shard, groupID, topic string, extras map[string]string, logger log.Logger) (*persqueue.WriterOptions, error) {
		lbEnv := recipe.New(t)

		writerOpts, err := DefaultWriterConfigFactory(config, shard, groupID, topic, extras, logger)
		if err != nil {
			return nil, xerrors.Errorf("unable to create writerOpts: %w", err)
		}

		endpoint := os.Getenv("YDB_ENDPOINT")
		database := os.Getenv("YDB_DATABASE")

		dsn := fmt.Sprintf("grpc://%s/?database=%s", endpoint, database)

		db, err := ydbsdk.Open(context.Background(), dsn)
		require.NoError(t, err)

		err = db.Topic().Create(context.Background(), writerOpts.Topic, topicoptions.CreateWithConsumer(topictypes.Consumer{
			Name:            lbEnv.DefaultConsumer,
			SupportedCodecs: []topictypes.Codec{topictypes.CodecRaw, topictypes.CodecGzip},
		}))
		require.NoError(t, err)

		port, err := strconv.Atoi(strings.Split(endpoint, ":")[1])
		if err != nil {
			return nil, xerrors.Errorf("unable to atoi port, endpoint:%s, err:%w", endpoint, err)
		}

		writerOpts.Endpoint = endpoint
		writerOpts.Port = port
		writerOpts.Database = database
		writerOpts.Credentials = nil
		writerOpts.Codec = persqueue.Raw

		result := writerOpts.WithProxy(endpoint)

		return &result, nil
	}
}

func NewTestSinkWithDefaultWriterFactory(t *testing.T, dst *LbDestination) abstract.Sinker {
	result, err := NewSinkWithFactory(
		dst,
		solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()}),
		logger.Log,
		"my_transfer_id",
		NewTestWriterConfigFactory(t),
		NewYDSWriterFactory,
		true,
		false)

	require.NoError(t, err)

	return result
}

//---------------------------------------------------------------------------------------------------------------------

func TestJSON(t *testing.T) {
	lbEnv := recipe.New(t)

	dst := &LbDestination{
		Instance: "sas.logbroker.yandex.net",
		Topic:    "local/foo_bar",
		Database: os.Getenv("YDB_DATABASE"),
		FormatSettings: server.SerializationFormat{
			Name: server.SerializationFormatJSON,
		},
		TLS: DisabledTLS,
	}
	dst.WithDefaults()

	testSink := NewTestSinkWithDefaultWriterFactory(t, dst)

	err := testSink.Push([]abstract.ChangeItem{*sinkTestTypicalChangeItem})
	require.NoError(t, err)

	dataExpected := []string{`{"id":1,"val":-8388605}`}
	dataCmp := func(in string, index int) bool {
		return dataExpected[index] == in
	}

	lbenv.CheckResult(t, lbEnv, dst.Database, dst.Topic, 1, dataCmp, lbenv.ComparatorStub, lbenv.ComparatorStub, lbenv.ComparatorStub, lbenv.ComparatorStub)

	require.NoError(t, testSink.Close())
}

func TestDebezium(t *testing.T) {
	lbEnv := recipe.New(t)

	dst := &LbDestination{
		Instance:    "sas.logbroker.yandex.net",
		TopicPrefix: "local/foo_bar",
		Database:    os.Getenv("YDB_DATABASE"),
		FormatSettings: server.SerializationFormat{
			Name: server.SerializationFormatDebezium,
			Settings: map[string]string{
				debeziumparameters.SourceType: "pg",
			},
		},
		TLS: DisabledTLS,
	}
	dst.WithDefaults()

	expectedTopicName := dst.TopicPrefix + ".public.basic_types15"

	v := `{"payload":{"after":{"id":1,"val":-8388605},"before":null,"op":"c","source":{"connector":"postgresql","db":"","lsn":25051056,"name":"local/foo_bar","schema":"public","snapshot":"false","table":"basic_types15","ts_ms":1643660670333,"txId":601,"version":"1.1.2.Final","xmin":null},"transaction":null,"ts_ms":1643660670333},"schema":{"fields":[{"field":"before","fields":[{"field":"id","optional":false,"type":"int32"},{"field":"val","optional":true,"type":"int32"}],"name":"local/foo_bar.public.basic_types15.Value","optional":true,"type":"struct"},{"field":"after","fields":[{"field":"id","optional":false,"type":"int32"},{"field":"val","optional":true,"type":"int32"}],"name":"local/foo_bar.public.basic_types15.Value","optional":true,"type":"struct"},{"field":"source","fields":[{"field":"version","optional":false,"type":"string"},{"field":"connector","optional":false,"type":"string"},{"field":"name","optional":false,"type":"string"},{"field":"ts_ms","optional":false,"type":"int64"},{"default":"false","field":"snapshot","name":"io.debezium.data.Enum","optional":true,"parameters":{"allowed":"true,last,false"},"type":"string","version":1},{"field":"db","optional":false,"type":"string"},{"field":"table","optional":false,"type":"string"},{"field":"lsn","optional":true,"type":"int64"},{"field":"schema","optional":false,"type":"string"},{"field":"txId","optional":true,"type":"int64"},{"field":"xmin","optional":true,"type":"int64"}],"name":"io.debezium.connector.postgresql.Source","optional":false,"type":"struct"},{"field":"op","optional":false,"type":"string"},{"field":"ts_ms","optional":true,"type":"int64"},{"field":"transaction","fields":[{"field":"id","optional":false,"type":"string"},{"field":"total_order","optional":false,"type":"int64"},{"field":"data_collection_order","optional":false,"type":"int64"}],"optional":true,"type":"struct"}],"name":"local/foo_bar.public.basic_types15.Envelope","optional":false,"type":"struct"}}`

	testSink := NewTestSinkWithDefaultWriterFactory(t, dst)

	err := testSink.Push([]abstract.ChangeItem{*sinkTestTypicalChangeItem})
	require.NoError(t, err)

	dataExpected := []string{v}
	dataCmp := func(in string, index int) bool {
		return dataExpected[index] == in
	}

	lbenv.CheckResult(t, lbEnv, dst.Database, expectedTopicName, 1, dataCmp, lbenv.ComparatorStub, lbenv.ComparatorStub, lbenv.ComparatorStub, lbenv.ComparatorStub)

	require.NoError(t, testSink.Close())
}

func TestMirror(t *testing.T) {
	lbEnv := recipe.New(t)

	dst := &LbDestination{
		Instance: "sas.logbroker.yandex.net",
		Topic:    "local/foo_bar",
		Database: os.Getenv("YDB_DATABASE"),
		FormatSettings: server.SerializationFormat{
			Name: server.SerializationFormatLbMirror,
		},
		TLS: DisabledTLS,
	}
	dst.WithDefaults()

	testSink := NewTestSinkWithDefaultWriterFactory(t, dst)

	err := testSink.Push([]abstract.ChangeItem{*sinkTestLbMirrorChangeItem})
	require.NoError(t, err)

	dataExpected := []string{"my_data"}
	dataCmp := func(in string, index int) bool {
		return dataExpected[index] == in
	}

	lbenv.CheckResult(t, lbEnv, dst.Database, dst.Topic, 1, dataCmp, lbenv.ComparatorStub, lbenv.ComparatorStub, lbenv.ComparatorStub, lbenv.ComparatorStub)

	require.NoError(t, testSink.Close())
}

func TestNative(t *testing.T) {
	lbEnv := recipe.New(t)

	dst := &LbDestination{
		Instance: "sas.logbroker.yandex.net",
		Topic:    "local/foo_bar",
		Database: os.Getenv("YDB_DATABASE"),
		FormatSettings: server.SerializationFormat{
			Name: server.SerializationFormatNative,
		},
		TLS: DisabledTLS,
	}
	dst.WithDefaults()

	testSink := NewTestSinkWithDefaultWriterFactory(t, dst)

	err := testSink.Push([]abstract.ChangeItem{*sinkTestTypicalChangeItem})
	require.NoError(t, err)

	dataCmp := func(in string, index int) bool {
		canonizedChangeItemBytes, _ := json.Marshal(sinkTestTypicalChangeItem)
		return in == "["+string(canonizedChangeItemBytes)+"]"
	}

	lbenv.CheckResult(t, lbEnv, dst.Database, dst.Topic, 1, dataCmp, lbenv.ComparatorStub, lbenv.ComparatorStub, lbenv.ComparatorStub, lbenv.ComparatorStub)

	require.NoError(t, testSink.Close())
}

//---------------------------------------------------------------------------------------------------------------------

func TestAddDTSystemTables(t *testing.T) {
	lbEnv := recipe.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dst := &LbDestination{
		Instance:    "sas.logbroker.yandex.net",
		TopicPrefix: "local/foo_bar",
		Database:    os.Getenv("YDB_DATABASE"),
		FormatSettings: server.SerializationFormat{
			Name: server.SerializationFormatDebezium,
			Settings: map[string]string{
				debeziumparameters.SourceType: "pg",
			},
		},
		TLS: DisabledTLS,
	}
	dst.WithDefaults()

	currChangeItem := *sinkTestTypicalChangeItem
	currChangeItem.Table = abstract.TableConsumerKeeper

	t.Run("false", func(t *testing.T) {
		dst1 := dst
		dst1.AddSystemTables = false

		testSink := NewTestSinkWithDefaultWriterFactory(t, dst1)

		err := testSink.Push([]abstract.ChangeItem{currChangeItem})
		require.NoError(t, err)

		lbenv.CheckResult(t, lbEnv, dst.Database, dst.TopicPrefix, 0, lbenv.ComparatorStub, lbenv.ComparatorStub, lbenv.ComparatorStub, lbenv.ComparatorStub, lbenv.ComparatorStub)

		require.NoError(t, testSink.Close())
	})

	t.Run("true", func(t *testing.T) {
		dst2 := dst
		dst2.AddSystemTables = true

		v := `{"payload":{"after":{"id":1,"val":-8388605},"before":null,"op":"c","source":{"connector":"postgresql","db":"","lsn":25051056,"name":"local/foo_bar","schema":"public","snapshot":"false","table":"__consumer_keeper","ts_ms":1643660670333,"txId":601,"version":"1.1.2.Final","xmin":null},"transaction":null,"ts_ms":1643660670333},"schema":{"fields":[{"field":"before","fields":[{"field":"id","optional":false,"type":"int32"},{"field":"val","optional":true,"type":"int32"}],"name":"local/foo_bar.public.__consumer_keeper.Value","optional":true,"type":"struct"},{"field":"after","fields":[{"field":"id","optional":false,"type":"int32"},{"field":"val","optional":true,"type":"int32"}],"name":"local/foo_bar.public.__consumer_keeper.Value","optional":true,"type":"struct"},{"field":"source","fields":[{"field":"version","optional":false,"type":"string"},{"field":"connector","optional":false,"type":"string"},{"field":"name","optional":false,"type":"string"},{"field":"ts_ms","optional":false,"type":"int64"},{"default":"false","field":"snapshot","name":"io.debezium.data.Enum","optional":true,"parameters":{"allowed":"true,last,false"},"type":"string","version":1},{"field":"db","optional":false,"type":"string"},{"field":"table","optional":false,"type":"string"},{"field":"lsn","optional":true,"type":"int64"},{"field":"schema","optional":false,"type":"string"},{"field":"txId","optional":true,"type":"int64"},{"field":"xmin","optional":true,"type":"int64"}],"name":"io.debezium.connector.postgresql.Source","optional":false,"type":"struct"},{"field":"op","optional":false,"type":"string"},{"field":"ts_ms","optional":true,"type":"int64"},{"field":"transaction","fields":[{"field":"id","optional":false,"type":"string"},{"field":"total_order","optional":false,"type":"int64"},{"field":"data_collection_order","optional":false,"type":"int64"}],"optional":true,"type":"struct"}],"name":"local/foo_bar.public.__consumer_keeper.Envelope","optional":false,"type":"struct"}}`

		testSink := NewTestSinkWithDefaultWriterFactory(t, dst2)

		err := testSink.Push([]abstract.ChangeItem{currChangeItem})
		require.NoError(t, err)

		dataExpected := []string{v}
		dataCmp := func(in string, index int) bool {
			return dataExpected[index] == in
		}

		systemTopicName := fmt.Sprintf("%s.public.%s", dst.TopicPrefix, abstract.TableConsumerKeeper)
		lbenv.CheckResult(t, lbEnv, dst.Database, systemTopicName, 1, dataCmp, lbenv.ComparatorStub, lbenv.ComparatorStub, lbenv.ComparatorStub, lbenv.ComparatorStub)

		require.NoError(t, testSink.Close())
	})
}

// TM-4075
// @nyoroon case, sharded replication lb(YQL-script)->lb with turned on setting: 'Split into sub tables'
// every table (sub table) should be written simultaneously (with own sourceID)
func TestSimultaneouslyWriteTables(t *testing.T) {
	lbEnv := recipe.New(t)

	dst := &LbDestination{
		Instance: "sas.logbroker.yandex.net",
		Topic:    "local/foo_bar",
		Database: os.Getenv("YDB_DATABASE"),
		FormatSettings: server.SerializationFormat{
			Name: server.SerializationFormatJSON,
		},
		TLS: DisabledTLS,
	}
	dst.WithDefaults()

	testSink := NewTestSinkWithDefaultWriterFactory(t, dst)

	err := testSink.Push([]abstract.ChangeItem{*sinkTestTypicalChangeItem})
	require.NoError(t, err)

	err = testSink.Push([]abstract.ChangeItem{*sinkTestTypicalChangeItem})
	require.NoError(t, err)

	lbenv.CheckResult(t, lbEnv, dst.Database, dst.Topic, 2, lbenv.ComparatorStub, lbenv.ComparatorStub, lbenv.ComparatorStub, lbenv.ComparatorStub, lbenv.ComparatorStub)

	require.NoError(t, testSink.Close())
}

func TestResetWriters(t *testing.T) {
	lbEnv := recipe.New(t)

	dst := &LbDestination{
		Instance: "sas.logbroker.yandex.net",
		Topic:    "local/foo_bar",
		Database: os.Getenv("YDB_DATABASE"),
		FormatSettings: server.SerializationFormat{
			Name: server.SerializationFormatNative,
		},
		TLS: DisabledTLS,
	}
	dst.WithDefaults()

	testSink := NewTestSinkWithDefaultWriterFactory(t, dst)

	err := testSink.Push([]abstract.ChangeItem{abstract.MakeSynchronizeEvent()})
	require.NoError(t, err)

	lbenv.CheckResult(t, lbEnv, dst.Database, dst.Topic, 0, lbenv.ComparatorStub, lbenv.ComparatorStub, lbenv.ComparatorStub, lbenv.ComparatorStub, lbenv.ComparatorStub)
	require.Equal(t, 0, len(testSink.(*sink).writers))

	require.NoError(t, testSink.Close())
}

//---------------------------------------------------------------------------------------------------------------------

type slowWriter struct {
	ctx            context.Context
	secondsToWrite int
	isSecondTry    bool
	writer         CancelableWriter
}

var _ CancelableWriter = (*slowWriter)(nil)

func (w *slowWriter) Write(ctx context.Context, data []byte) error {
	if !w.isSecondTry {
		select {
		case <-w.ctx.Done():
			return context.Canceled
		case <-time.After(time.Second * time.Duration(w.secondsToWrite)):
			return nil
		}
	} else {
		return w.writer.Write(ctx, data)
	}
}

func (w *slowWriter) Close(ctx context.Context) error {
	return w.writer.Close(ctx)
}

func (w *slowWriter) Cancel() {
	w.writer.Cancel()
}

func (w *slowWriter) GroupID() string {
	return w.writer.GroupID()
}

func (w *slowWriter) SetGroupID(groupID string) {
	w.writer.SetGroupID(groupID)
}

func TestLbRetryableTimeout(t *testing.T) {
	lbEnv := recipe.New(t)

	dst := &LbDestination{
		Instance:        "sas.logbroker.yandex.net",
		Topic:           "local/foo_bar",
		Database:        os.Getenv("YDB_DATABASE"),
		WriteTimeoutSec: 2,
		FormatSettings: server.SerializationFormat{
			Name: server.SerializationFormatJSON,
		},
		TLS: DisabledTLS,
	}
	dst.WithDefaults()

	var writer CancelableWriter
	factoryCalls := 0

	testSink, err := NewSinkWithFactory(
		dst,
		solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()}),
		logger.Log,
		"my_transfer_id",
		NewTestWriterConfigFactory(t),
		func(opts *persqueue.WriterOptions, logger log.Logger) (CancelableWriter, error) {
			ydsWriter, err := NewYDSWriterFactory(opts, logger)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())

			w := ydsWriter.(*topicWriter)
			w.cancelFunc = cancel

			factoryCalls++

			writer = &slowWriter{
				ctx:            ctx,
				secondsToWrite: 999,
				isSecondTry:    factoryCalls == 2,
				writer:         w,
			}

			return writer, nil
		},
		true,
		false,
	)
	require.NoError(t, err)

	err = testSink.Push([]abstract.ChangeItem{*sinkTestTypicalChangeItem})
	require.Error(t, err)

	err = testSink.Push([]abstract.ChangeItem{*sinkTestTypicalChangeItem})
	require.NoError(t, err)

	lbenv.CheckResult(t, lbEnv, dst.Database, dst.Topic, 1, lbenv.ComparatorStub, lbenv.ComparatorStub, lbenv.ComparatorStub, lbenv.ComparatorStub, lbenv.ComparatorStub)
	require.NoError(t, testSink.Close())
	require.Equal(t, 2, factoryCalls)
}

//---------------------------------------------------------------------------------------------------------------------
// synchronizer event

// test just one synchronize event - nothing should be written into the logbroker
func TestSynchronizerEvent1(t *testing.T) {
	_ = recipe.New(t)

	dst := &LbDestination{
		Instance: "sas.logbroker.yandex.net",
		Topic:    "local/foo_bar",
		Database: os.Getenv("YDB_DATABASE"),
		FormatSettings: server.SerializationFormat{
			Name: server.SerializationFormatNative,
		},
		TLS: DisabledTLS,
	}
	dst.WithDefaults()

	noOpWriterFactory := func(_ *persqueue.WriterOptions, _ log.Logger) (CancelableWriter, error) {
		return nil, xerrors.Errorf("never should be called")
	}

	testSink, err := NewSinkWithFactory(
		dst,
		solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()}),
		logger.Log,
		"my_transfer_id",
		NewTestWriterConfigFactory(t),
		noOpWriterFactory,
		true,
		false,
	)
	require.NoError(t, err)

	err = testSink.Push([]abstract.ChangeItem{abstract.MakeSynchronizeEvent()})
	require.NoError(t, err)
	require.NoError(t, testSink.Close())
}

// test one data event + one synchronize event - only data event should be written into the logbroker
func TestSynchronizerEvent2(t *testing.T) {
	lbEnv := recipe.New(t)

	dst := &LbDestination{
		Instance: "sas.logbroker.yandex.net",
		Topic:    "local/foo_bar",
		Database: os.Getenv("YDB_DATABASE"),
		FormatSettings: server.SerializationFormat{
			Name: server.SerializationFormatNative,
		},
		TLS: DisabledTLS,
	}
	dst.WithDefaults()

	testSink := NewTestSinkWithDefaultWriterFactory(t, dst)

	err := testSink.Push([]abstract.ChangeItem{*sinkTestTypicalChangeItem, abstract.MakeSynchronizeEvent()})
	require.NoError(t, err)

	dataCmp := func(in string, index int) bool {
		canonizedChangeItemBytes, _ := json.Marshal(sinkTestTypicalChangeItem)
		return in == "["+string(canonizedChangeItemBytes)+"]"
	}

	lbenv.CheckResult(t, lbEnv, dst.Database, dst.Topic, 1, dataCmp, lbenv.ComparatorStub, lbenv.ComparatorStub, lbenv.ComparatorStub, lbenv.ComparatorStub)

	require.NoError(t, testSink.Close())
}
