package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/metrics"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers"
	_ "github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers/registry"
	audittrailsv1engine "github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers/registry/audittrailsv1/engine"
	cloudeventsengine "github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers/registry/cloudevents/engine"
	cloudloggingengine "github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers/registry/cloudlogging/engine"
	confluentschemaregistryengine "github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers/registry/confluentschemaregistry/engine"
	debeziumengine "github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers/registry/debezium/engine"
	jsonparser "github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers/registry/json"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers/registry/protobuf/protoparser"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers/registry/protobuf/protoparser/gotest/prototest"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers/registry/protobuf/protoscanner"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/stats"
	confluentsrmock "github.com/doublecloud/transfer/transfer_manager/go/tests/helpers/confluent_schema_registry_mock"
	"github.com/stretchr/testify/require"
)

// TestCanonizeParserConfigsList
// We need canonize parserConfig names, bcs for now they work via reflect dispatching,
// but it will break production, if someone renames parserConfig struct.
func TestCanonizeParserConfigsList(t *testing.T) {
	parserConfigs := parsers.KnownParsersConfigs()
	parsersConfigsMap := make(map[string]bool)
	for _, parserConfig := range parserConfigs {
		parsersConfigsMap[parserConfig] = true
	}

	canonizedParsersConfigsNames := []string{
		"audit_trails_v1.common",
		"blank.lb",
		"cloud_events.common",
		"cloud_events.lb",
		"cloud_logging.common",
		"debezium.common",
		"debezium.lb",
		"json.common",
		"json.lb",
		"logfeller.lb",
		"native.lb",
		"tskv.common",
		"tskv.lb",
		"yql.lb",
		"proto.lb",
		"proto.common",
	}

	for _, expectedParserConfigName := range canonizedParsersConfigsNames {
		require.True(t, parsersConfigsMap[expectedParserConfigName])
	}
}

func TestParserConfigsList(t *testing.T) {
	fmt.Println("known parser configs (alphabetically):")
	for _, parser := range parsers.KnownParsersConfigs() {
		fmt.Printf("\t%s\n", parser)
	}

	fmt.Println("known parsers (alphabetically):")
	for _, parser := range parsers.KnownParsers() {
		fmt.Printf("\t%s\n", parser)
	}
}

func TestUnparsed(t *testing.T) {
	abstractPartition := abstract.Partition{
		Cluster:   "lbkx",
		Topic:     "a/b@c",
		Partition: 1,
	}

	checkEx := func(t *testing.T, parser parsers.Parser, msg persqueue.ReadMessage) {
		changeItems := parser.Do(msg, abstractPartition)
		require.Equal(t, 1, len(changeItems))
		require.Equal(t, "a_b_c_unparsed", changeItems[0].Table)
	}

	//---

	t.Run("audittrailsv1", func(t *testing.T) {
		parser := audittrailsv1engine.NewAuditTrailsV1ParserImpl(true, false, logger.Log, stats.NewSourceStats(metrics.NewRegistry()))
		checkEx(t, parser, persqueue.ReadMessage{Data: []byte("{]")})
	})
	// 'blank' parser can't generate 'unparsed'
	t.Run("cloudevents", func(t *testing.T) {
		schemaRegistryMock := confluentsrmock.NewConfluentSRMock(nil, nil)
		defer schemaRegistryMock.Close()
		parser := cloudeventsengine.NewCloudEventsImpl("", "uname", "pass", "", false, logger.Log, func(in string) string { return schemaRegistryMock.URL() })
		checkEx(t, parser, persqueue.ReadMessage{Data: []byte("{]")})
	})
	t.Run("cloudlogging", func(t *testing.T) {
		parser := cloudloggingengine.NewCloudLoggingImpl(false, logger.Log, stats.NewSourceStats(metrics.NewRegistry()))
		checkEx(t, parser, persqueue.ReadMessage{Data: []byte("{]")})
	})
	t.Run("confluentschemaregistry", func(t *testing.T) {
		schemaRegistryMock := confluentsrmock.NewConfluentSRMock(nil, nil)
		defer schemaRegistryMock.Close()
		parser := confluentschemaregistryengine.NewConfluentSchemaRegistryImpl(schemaRegistryMock.URL(), "", "uname", "pass", false, logger.Log)
		checkEx(t, parser, persqueue.ReadMessage{Data: []byte("{]")})
	})
	t.Run("debezium", func(t *testing.T) {
		parser := debeziumengine.NewDebeziumImpl(logger.Log, nil, 1)
		checkEx(t, parser, persqueue.ReadMessage{Data: []byte("{]")})
	})
	t.Run("json", func(t *testing.T) {
		parserConfigJSONCommon := &jsonparser.ParserConfigJSONCommon{
			Fields: []abstract.ColSchema{
				{ColumnName: "id", Required: true},
			},
			SchemaResourceName: "",
			NullKeysAllowed:    false,
			AddRest:            true,
			AddDedupeKeys:      false,
		}
		parser, err := jsonparser.NewParserJSON(parserConfigJSONCommon, false, logger.Log, stats.NewSourceStats(metrics.NewRegistry()))
		require.NoError(t, err)
		checkEx(t, parser, persqueue.ReadMessage{Data: []byte("{]")})
	})
	// 'logfeller' parser - is able to generate 'unparsed', but it's CGO - so, I will skip this check here
	// 'native' parser can't generate 'unparsed'
	t.Run("protobuf", func(t *testing.T) {
		var stdDataTypesFilled = &prototest.StdDataTypesMsg{}

		pMsg := persqueue.ReadMessage{
			Offset:      1,
			SeqNo:       1,
			SourceID:    nil,
			CreateTime:  time.Time{},
			WriteTime:   time.Time{},
			IP:          "",
			Data:        []byte("im-invalid-proto-message"),
			Codec:       persqueue.Codec(0),
			ExtraFields: nil,
		}

		desc := stdDataTypesFilled.ProtoReflect().Descriptor()
		config := protoparser.ProtoParserConfig{
			ProtoMessageDesc:   desc,
			ScannerMessageDesc: desc,
			ProtoScannerType:   protoscanner.ScannerTypeLineSplitter,
			LineSplitter:       abstract.LfLineSplitterDoNotSplit,
		}

		parser, err := protoparser.NewProtoParser(&config, stats.NewSourceStats(metrics.NewRegistry()))
		require.NoError(t, err)
		checkEx(t, parser, pMsg)
	})
	// 'tskv' parser can't generate 'unparsed'
	// 'yql' parser - maybe generates 'unparsed', maybe not - but it's CGO - so, I will skip this check here
}
