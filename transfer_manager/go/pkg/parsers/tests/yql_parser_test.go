//go:build cgo && yql
// +build cgo,yql

package tests

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue"
	"github.com/doublecloud/tross/library/go/core/resource"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/internal/metrics"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/base/events"
	dpconfig "github.com/doublecloud/tross/transfer_manager/go/pkg/config/dataplane"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/format"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/parsers"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/parsers/registry/yql"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/parsers/registry/yql/engine"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/parsers/tests/samples"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/stats"
	"github.com/stretchr/testify/require"
)

func init() {
	dpconfig.InternalCloud = &dpconfig.InternalCloudConfig{}
}

var (
	fakePartition = abstract.Partition{
		Cluster:   "logbroker",
		Partition: 0,
		Topic:     "test/fake",
	}
)

func GetYqlParserImpl(in parsers.Parser) *engine.YqlParser {
	wrapper := in.(parsers.WrappedParser)
	return wrapper.Unwrap().(*engine.YqlParser)
}

func TestYqlParser_ComplexNelLog(t *testing.T) {
	parserConfigMap := ParserConfigMap(t, samples.NelSample)
	parserConfigUnwrapped := parserConfigMapToStruct(t, parserConfigMap).(*yql.ParserConfigYQLLb)
	parserConfigUnwrapped.YqlScript = strings.ReplaceAll(string(resource.Get("yql_parser.sql")), `FileContent("parser")`, `"LogFellerParsers.ParseTskv"`) + `
$type = List<
   Struct<
       age: Int32,
       body: Struct<
           elapsed_time: Int32,
           method: String,
           phase: String,
           protocol: String,
           referrer: String,
           sampling_fraction: Double,
           server_ip: String,
           status_code: Uint32,
           type: String
       >?,
       type: String,
       url: String,
       user_agent: String?
   >
>;
SELECT addr AS remote_addr,
content,
Yson::ParseJson(content, Yson::Options(false AS Strict)) AS event
FROM $rows;
`
	parser, err := parsers.NewParserFromParserConfig(parserConfigUnwrapped, "", false, logger.Log, stats.NewSourceStats(metrics.NewRegistry()))
	require.NoError(t, err)
	res := parser.Do(samples.Data[samples.NelSample], fakePartition)
	abstract.Dump(res)
	require.Equal(t, 7, len(res))
}

func TestYqlParser_DoTskv(t *testing.T) {
	parserConfigMap := ParserConfigMap(t, samples.TaxiYqlSample)
	parserConfigUnwrapped := parserConfigMapToStruct(t, parserConfigMap).(*yql.ParserConfigYQLLb)
	parserConfigUnwrapped.YqlScript = strings.ReplaceAll(string(resource.Get("yql_parser.sql")), `FileContent("parser")`, `"LogFellerParsers.ParseTskv"`) + "SELECT * FROM $rows;"
	logger.Log.Infof("Data[TaxiSample]: %v", string(samples.Data[samples.TaxiSample].Data))
	parser, err := parsers.NewParserFromParserConfig(parserConfigUnwrapped, "", false, logger.Log, stats.NewSourceStats(metrics.NewRegistry()))
	require.NoError(t, err)
	res := parser.Do(samples.Data[samples.TaxiSample], fakePartition)
	abstract.Dump(res)
	require.Equal(t, 5, len(res))
}

func TestYqlParser_DoPostProcess(t *testing.T) {
	parserConfigMap := ParserConfigMap(t, samples.TaxiYqlSample)
	parserConfigUnwrapped := parserConfigMapToStruct(t, parserConfigMap).(*yql.ParserConfigYQLLb)
	parserConfigUnwrapped.YqlScript = strings.ReplaceAll(string(resource.Get("yql_parser.sql")), `FileContent("parser")`, `"LogFellerParsers.ParseTskv"`) + "SELECT level, module, text FROM $rows;"
	parser, err := parsers.NewParserFromParserConfig(parserConfigUnwrapped, "", false, logger.Log, stats.NewSourceStats(metrics.NewRegistry()))
	require.NoError(t, err)
	res := parser.Do(samples.Data[samples.TaxiSample], fakePartition)
	abstract.Dump(res)
	require.Equal(t, 5, len(res))
}

func TestYqlParser_Filter(t *testing.T) {
	parserConfigMap := ParserConfigMap(t, samples.TaxiYqlSample)
	parserConfigUnwrapped := parserConfigMapToStruct(t, parserConfigMap).(*yql.ParserConfigYQLLb)
	parserConfigUnwrapped.YqlScript = strings.ReplaceAll(string(resource.Get("yql_parser.sql")), `FileContent("parser")`, `"LogFellerParsers.ParseTskv"`) + "SELECT level, module, text FROM $rows where level != 'INFO';"
	parser, err := parsers.NewParserFromParserConfig(parserConfigUnwrapped, "", false, logger.Log, stats.NewSourceStats(metrics.NewRegistry()))
	require.NoError(t, err)
	res := parser.Do(samples.Data[samples.TaxiSample], fakePartition)
	abstract.Dump(res)
	require.Equal(t, 0, len(res))
}

func TestYqlParser_DoJson(t *testing.T) {
	parserConfigMap := ParserConfigMap(t, samples.TM280YqlSample)
	parserConfigUnwrapped := parserConfigMapToStruct(t, parserConfigMap).(*yql.ParserConfigYQLLb)
	parserConfigUnwrapped.YqlScript = strings.ReplaceAll(string(resource.Get("yql_parser.sql")), `FileContent("parser")`, `"LogFellerParsers.ParseJson"`) + "SELECT * FROM $rows;"
	parser, err := parsers.NewParserFromParserConfig(parserConfigUnwrapped, "", false, logger.Log, stats.NewSourceStats(metrics.NewRegistry()))
	require.NoError(t, err)
	res := parser.Do(samples.Data[samples.TM280Sample], fakePartition)
	abstract.Dump(res)
	require.Equal(t, 2, len(res))
}

func TestYqlParser_DoSpecs(t *testing.T) {
	for _, testCase := range []string{samples.TaxiYqlSample, samples.TskvYqlSample, samples.JSONYqlSample} {
		parserConfigMap := ParserConfigMap(t, testCase)
		parserConfigUnwrapped := parserConfigMapToStruct(t, parserConfigMap).(*yql.ParserConfigYQLLb)
		parserConfigUnwrapped.YqlScript = strings.ReplaceAll(string(resource.Get("yql_parser.sql")), `FileContent("parser")`, `"LogFellerParsers.ParseTskv"`) + "SELECT * FROM $rows;"
		if testCase == samples.JSONSample {
			parserConfigUnwrapped.YqlScript = strings.ReplaceAll(string(resource.Get("yql_parser.sql")), `FileContent("parser")`, `"LogFellerParsers.ParseJson"`) + "SELECT * FROM $rows;"
		}
		parser, err := parsers.NewParserFromParserConfig(parserConfigUnwrapped, "", false, logger.Log, stats.NewSourceStats(metrics.NewRegistry()))
		require.NoError(t, err)
		for _, size := range []int{1, 5, 100} {
			t.Run(fmt.Sprint(testCase, "/", size), func(t *testing.T) {
				d := samples.Data[testCase]
				batch := persqueue.MessageBatch{
					Messages: make([]persqueue.ReadMessage, size),
				}
				st := time.Now()
				for i := 0; i < size; i++ {
					batch.Messages[i] = d
				}
				res := parser.DoBatch(batch)
				require.True(t, len(res) > 0)
				fmt.Printf("Case: %v, size: %v resulted %v rows in %v\n", fmt.Sprint(testCase, "/", size), format.SizeInt(len(d.Data)*size), len(res), time.Since(st))
			})
		}
	}
}

func TestYqlParser_DoSpecs_abstract2(t *testing.T) {
	for _, testCase := range []string{samples.TaxiYqlSample, samples.TskvYqlSample, samples.JSONYqlSample} {
		parserConfigMap := ParserConfigMap(t, testCase)
		parserConfigUnwrapped := parserConfigMapToStruct(t, parserConfigMap).(*yql.ParserConfigYQLLb)
		parserConfigUnwrapped.YqlScript = strings.ReplaceAll(string(resource.Get("yql_parser.sql")), `FileContent("parser")`, `"LogFellerParsers.ParseTskv"`) + "SELECT * FROM $rows;"
		if testCase == samples.JSONSample {
			parserConfigUnwrapped.YqlScript = strings.ReplaceAll(string(resource.Get("yql_parser.sql")), `FileContent("parser")`, `"LogFellerParsers.ParseJson"`) + "SELECT * FROM $rows;"
		}
		parser, err := parsers.NewParserFromParserConfig(parserConfigUnwrapped, "", false, logger.Log, stats.NewSourceStats(metrics.NewRegistry()))
		require.NoError(t, err)
		parserUnwrapped := GetYqlParserImpl(parser)

		for _, size := range []int{1, 5, 100} {
			t.Run(fmt.Sprint(testCase, "/", size), func(t *testing.T) {
				d := samples.Data[testCase]
				batch := persqueue.MessageBatch{
					Messages: make([]persqueue.ReadMessage, size),
				}
				st := time.Now()
				for i := 0; i < size; i++ {
					batch.Messages[i] = d
				}
				res := parserUnwrapped.ParseBatch(batch)
				var changes []abstract.ChangeItem
				for res.Next() {
					ev, err := res.Event()
					require.NoError(t, err)
					iev, ok := ev.(events.InsertEvent)
					require.True(t, ok)
					ci, err := iev.ToOldChangeItem()
					require.NoError(t, err)
					changes = append(changes, *ci)
				}
				require.True(t, len(changes) > 0)
				fmt.Printf("Case: %v, size: %v resulted %v rows in %v\n", fmt.Sprint(testCase, "/", size), format.SizeInt(len(d.Data)*size), len(changes), time.Since(st))
				if len(changes) > 5 {
					changes = changes[:5]
				}
				abstract.Dump(changes)
			})
		}
	}
}

func TestYqlParser_ComplexPrimaryKey(t *testing.T) { // test case from TM-5115
	parserConfigMap := ParserConfigMap(t, samples.YQLComplexPrimaryKey)
	parserConfigUnwrapped := parserConfigMapToStruct(t, parserConfigMap).(*yql.ParserConfigYQLLb)

	t.Run("original order: [timestamp;id]", func(t *testing.T) {
		parser, err := parsers.NewParserFromParserConfig(parserConfigUnwrapped, "", false, logger.Log, stats.NewSourceStats(metrics.NewRegistry()))
		require.NoError(t, err)
		parserUnwrapped := GetYqlParserImpl(parser)
		outputSchema := parserUnwrapped.Output()

		colNameToIndex := abstract.MakeMapColNameToIndex(outputSchema)
		require.Contains(t, colNameToIndex, "timestamp")
		require.Contains(t, colNameToIndex, "id")
		require.True(t, colNameToIndex["timestamp"] < colNameToIndex["id"])
	})

	t.Run("reverse order: [id;timestamp]", func(t *testing.T) {
		// make inverted order
		parserConfigUnwrapped.Fields[0], parserConfigUnwrapped.Fields[1] = parserConfigUnwrapped.Fields[1], parserConfigUnwrapped.Fields[0]
		parserConfigUnwrapped.YqlOutputKeys[0], parserConfigUnwrapped.YqlOutputKeys[1] = parserConfigUnwrapped.YqlOutputKeys[1], parserConfigUnwrapped.YqlOutputKeys[0]

		parser, err := parsers.NewParserFromParserConfig(parserConfigUnwrapped, "", false, logger.Log, stats.NewSourceStats(metrics.NewRegistry()))
		require.NoError(t, err)
		parserUnwrapped := GetYqlParserImpl(parser)
		outputSchema := parserUnwrapped.Output()

		colNameToIndex := abstract.MakeMapColNameToIndex(outputSchema)
		require.Contains(t, colNameToIndex, "id")
		require.Contains(t, colNameToIndex, "timestamp")
		require.True(t, colNameToIndex["id"] < colNameToIndex["timestamp"])
	})
}

func TestYqlParser_CheckOutputSchemaOrderIsStable(t *testing.T) { // test case from DTSUPPORT-1952
	parserConfigMap := ParserConfigMap(t, samples.YQLComplexPrimaryKey)
	parserConfigUnwrapped := parserConfigMapToStruct(t, parserConfigMap).(*yql.ParserConfigYQLLb)
	parser0, err := parsers.NewParserFromParserConfig(parserConfigUnwrapped, "", false, logger.Log, stats.NewSourceStats(metrics.NewRegistry()))
	require.NoError(t, err)
	parserUnwrapped0 := GetYqlParserImpl(parser0)
	outputSchema0 := parserUnwrapped0.Output()

	parser1, err := parsers.NewParserFromParserConfig(parserConfigUnwrapped, "", false, logger.Log, stats.NewSourceStats(metrics.NewRegistry()))
	require.NoError(t, err)
	parserUnwrapped1 := GetYqlParserImpl(parser1)
	outputSchema1 := parserUnwrapped1.Output()

	require.Equal(t, outputSchema0, outputSchema1)
}

func BenchmarkYqlParser_Do(b *testing.B) {
	for _, testCase := range []string{samples.TskvSample, samples.JSONSample} {
		for _, size := range []int{1, 5, 100} {
			b.Run(fmt.Sprint(testCase, "/", size), func(b *testing.B) {
				parser, err := parsers.NewParserFromMap(ParserConfigMapB(b, testCase), "", false, logger.Log, stats.NewSourceStats(metrics.NewRegistry()))
				require.NoError(b, err)
				d := samples.Data[testCase]
				batch := persqueue.MessageBatch{
					Messages: make([]persqueue.ReadMessage, size),
				}
				tSize := 0
				for i := 0; i < size; i++ {
					batch.Messages[i] = d
					tSize += len(d.Data)
				}
				logger.Log.Infof("input size: %v", format.SizeInt(tSize))
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					r := parser.DoBatch(batch)
					require.True(b, len(r) > 0)
				}
				b.SetBytes(int64(len(d.Data) * b.N * size))
				b.ReportAllocs()
			})
		}
	}
}
