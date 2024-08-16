package tests

import (
	"fmt"
	"testing"

	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers/registry/debezium"
	jsonparser "github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers/registry/json"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/airbyte"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/clickhouse/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/eventhub"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/greenplum"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/kafka"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/logbroker"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/mongo"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/mysql"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/oracle"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/postgres"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/ydb"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/yds"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/yt"
	"github.com/stretchr/testify/require"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func parserJSONCommon(t *testing.T) map[string]interface{} {
	parserConfigStruct := &jsonparser.ParserConfigJSONCommon{
		Fields: []abstract.ColSchema{
			{ColumnName: "msg", DataType: ytschema.TypeString.String()},
		},
	}
	parserConfigMap, err := parsers.ParserConfigStructToMap(parserConfigStruct)
	require.NoError(t, err)
	return parserConfigMap
}

func parserDebeziumCommon(t *testing.T) map[string]interface{} {
	parserConfigStruct := &debezium.ParserConfigDebeziumCommon{}
	parserConfigMap, err := parsers.ParserConfigStructToMap(parserConfigStruct)
	require.NoError(t, err)
	return parserConfigMap
}

func checkDst(t *testing.T, src server.Source, serializerName server.SerializationFormatName, transferType abstract.TransferType, expectedOk bool) {
	dst := kafka.KafkaDestination{FormatSettings: server.SerializationFormat{Name: serializerName}}
	if expectedOk {
		require.NoError(t, dst.Compatible(src, transferType))
	} else {
		require.Error(t, dst.Compatible(src, transferType))
	}
}

func TestSourceCompatible(t *testing.T) {
	type testCase struct {
		src                         server.Source
		serializationFormat         server.SerializationFormatName
		expectedOk                  bool
		inferredSerializationFormat server.SerializationFormatName
	}

	testCases := []testCase{
		{&logbroker.LfSource{ParserConfig: nil}, server.SerializationFormatMirror, false, ""},
		{&logbroker.LfSource{ParserConfig: parserJSONCommon(t)}, server.SerializationFormatMirror, false, ""},
		{&logbroker.LfSource{ParserConfig: parserDebeziumCommon(t)}, server.SerializationFormatMirror, false, ""},

		{&logbroker.LfSource{ParserConfig: nil}, server.SerializationFormatJSON, false, ""},
		{&logbroker.LfSource{ParserConfig: parserJSONCommon(t)}, server.SerializationFormatJSON, true, server.SerializationFormatJSON},
		{&logbroker.LfSource{ParserConfig: parserDebeziumCommon(t)}, server.SerializationFormatJSON, false, ""},

		{&logbroker.LfSource{ParserConfig: nil}, server.SerializationFormatDebezium, false, ""},
		{&logbroker.LfSource{ParserConfig: parserJSONCommon(t)}, server.SerializationFormatDebezium, false, ""},
		{&logbroker.LfSource{ParserConfig: parserDebeziumCommon(t)}, server.SerializationFormatDebezium, false, ""},

		{&kafka.KafkaSource{ParserConfig: nil}, server.SerializationFormatMirror, true, server.SerializationFormatMirror},
		{&kafka.KafkaSource{ParserConfig: parserJSONCommon(t)}, server.SerializationFormatMirror, false, ""},
		{&kafka.KafkaSource{ParserConfig: parserDebeziumCommon(t)}, server.SerializationFormatMirror, false, ""},

		{&kafka.KafkaSource{ParserConfig: nil}, server.SerializationFormatJSON, false, ""},
		{&kafka.KafkaSource{ParserConfig: parserJSONCommon(t)}, server.SerializationFormatJSON, true, server.SerializationFormatJSON},
		{&kafka.KafkaSource{ParserConfig: parserDebeziumCommon(t)}, server.SerializationFormatJSON, false, ""},

		{&kafka.KafkaSource{ParserConfig: nil}, server.SerializationFormatDebezium, false, ""},
		{&kafka.KafkaSource{ParserConfig: parserJSONCommon(t)}, server.SerializationFormatDebezium, false, ""},
		{&kafka.KafkaSource{ParserConfig: parserDebeziumCommon(t)}, server.SerializationFormatDebezium, false, ""},

		{&eventhub.EventHubSource{ParserConfig: nil}, server.SerializationFormatMirror, true, server.SerializationFormatMirror},
		{&eventhub.EventHubSource{ParserConfig: parserJSONCommon(t)}, server.SerializationFormatMirror, false, ""},
		{&eventhub.EventHubSource{ParserConfig: parserDebeziumCommon(t)}, server.SerializationFormatMirror, false, ""},

		{&eventhub.EventHubSource{ParserConfig: nil}, server.SerializationFormatJSON, false, ""},
		{&eventhub.EventHubSource{ParserConfig: parserJSONCommon(t)}, server.SerializationFormatJSON, true, server.SerializationFormatJSON},
		{&eventhub.EventHubSource{ParserConfig: parserDebeziumCommon(t)}, server.SerializationFormatJSON, false, ""},

		{&eventhub.EventHubSource{ParserConfig: nil}, server.SerializationFormatDebezium, false, ""},
		{&eventhub.EventHubSource{ParserConfig: parserJSONCommon(t)}, server.SerializationFormatDebezium, false, ""},
		{&eventhub.EventHubSource{ParserConfig: parserDebeziumCommon(t)}, server.SerializationFormatDebezium, false, ""},

		{&yds.YDSSource{ParserConfig: nil}, server.SerializationFormatMirror, true, server.SerializationFormatMirror},
		{&yds.YDSSource{ParserConfig: parserJSONCommon(t)}, server.SerializationFormatMirror, false, ""},
		{&yds.YDSSource{ParserConfig: parserDebeziumCommon(t)}, server.SerializationFormatMirror, false, ""},

		{&yds.YDSSource{ParserConfig: nil}, server.SerializationFormatJSON, false, ""},
		{&yds.YDSSource{ParserConfig: parserJSONCommon(t)}, server.SerializationFormatJSON, true, server.SerializationFormatJSON},
		{&yds.YDSSource{ParserConfig: parserDebeziumCommon(t)}, server.SerializationFormatJSON, false, ""},

		{&yds.YDSSource{ParserConfig: nil}, server.SerializationFormatDebezium, false, ""},
		{&yds.YDSSource{ParserConfig: parserJSONCommon(t)}, server.SerializationFormatDebezium, false, ""},
		{&yds.YDSSource{ParserConfig: parserDebeziumCommon(t)}, server.SerializationFormatDebezium, false, ""},

		{&postgres.PgSource{}, server.SerializationFormatMirror, false, ""},
		{&postgres.PgSource{}, server.SerializationFormatJSON, false, ""},
		{&postgres.PgSource{}, server.SerializationFormatDebezium, true, server.SerializationFormatDebezium},

		{&mysql.MysqlSource{}, server.SerializationFormatMirror, false, ""},
		{&mysql.MysqlSource{}, server.SerializationFormatJSON, false, ""},
		{&mysql.MysqlSource{}, server.SerializationFormatDebezium, true, server.SerializationFormatDebezium},

		{&ydb.YdbSource{}, server.SerializationFormatMirror, false, ""},
		{&ydb.YdbSource{}, server.SerializationFormatJSON, false, ""},
		{&ydb.YdbSource{}, server.SerializationFormatDebezium, true, server.SerializationFormatDebezium},

		{&airbyte.AirbyteSource{}, server.SerializationFormatMirror, false, ""},
		{&airbyte.AirbyteSource{}, server.SerializationFormatJSON, true, server.SerializationFormatJSON},
		{&airbyte.AirbyteSource{}, server.SerializationFormatDebezium, false, ""},

		{&model.ChSource{}, server.SerializationFormatMirror, false, ""},
		{&model.ChSource{}, server.SerializationFormatJSON, false, ""},
		{&model.ChSource{}, server.SerializationFormatDebezium, false, ""},

		{&greenplum.GpSource{}, server.SerializationFormatMirror, false, ""},
		{&greenplum.GpSource{}, server.SerializationFormatJSON, false, ""},
		{&greenplum.GpSource{}, server.SerializationFormatDebezium, false, ""},

		{&mongo.MongoSource{}, server.SerializationFormatMirror, false, ""},
		{&mongo.MongoSource{}, server.SerializationFormatJSON, false, ""},
		{&mongo.MongoSource{}, server.SerializationFormatDebezium, false, ""},

		{&oracle.OracleSource{}, server.SerializationFormatMirror, false, ""},
		{&oracle.OracleSource{}, server.SerializationFormatJSON, false, ""},
		{&oracle.OracleSource{}, server.SerializationFormatDebezium, false, ""},

		{&yt.YtSource{}, server.SerializationFormatMirror, false, ""},
		{&yt.YtSource{}, server.SerializationFormatJSON, false, ""},
		{&yt.YtSource{}, server.SerializationFormatDebezium, false, ""},
	}

	for i, el := range testCases {
		fmt.Println(i)
		checkDst(t, el.src, el.serializationFormat, abstract.TransferTypeIncrementOnly, el.expectedOk)
		if el.expectedOk {
			require.Equal(t, el.inferredSerializationFormat, kafka.InferFormatSettings(el.src, server.SerializationFormat{Name: server.SerializationFormatAuto}).Name)
		} else {
			require.Equal(t, string(el.inferredSerializationFormat), "")
		}
	}
}
