package protoparser

import (
	"fmt"
	"testing"
	"time"

	"github.com/doublecloud/transfer/library/go/core/metrics/mock"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers/registry/protobuf/protoparser/gotest/prototest"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers/registry/protobuf/protoscanner"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/stats"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/schema"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var stdDataTypesFilled = &prototest.StdDataTypesMsg{
	DoubleField:   1.11,
	FloatField:    2.2,
	Int32Field:    2,
	Int64Field:    3,
	Uint32Field:   4,
	Uint64Field:   5,
	Sint32Field:   6,
	Sint64Field:   7,
	Fixed32Field:  8,
	Fixed64Field:  9,
	Sfixed32Field: 10,
	Sfixed64Field: 11,
	BoolField:     true,
	StringField:   "string",
	BytesField:    []byte("bytes"),
	MapField: map[string]int32{
		"key1": 23,
		"key2": 12,
	},
	RepeatedField: []string{"1", "2", "3"},
	MsgField: &prototest.EmbeddedMsg{
		StringField: "stringField",
		Int32Field:  2,
		EnumField:   prototest.EmbeddedEnum_ITEM_2,
	},
}

var stdDataTypesEmpty = &prototest.StdDataTypesMsg{
	DoubleField: 2.223,
}

func getSourceStatsMock() *stats.SourceStats {
	return stats.NewSourceStats(mock.NewRegistry(mock.NewRegistryOpts()))
}

func checkColsEqual(t *testing.T, items []abstract.ChangeItem) {
	for _, ci := range items {
		require.Equal(t, len(ci.ColumnNames), len(ci.ColumnValues))
		require.Equal(t, len(ci.ColumnNames), len(ci.TableSchema.Columns()))

		for i, cs := range ci.TableSchema.Columns() {
			require.Equal(t, cs.ColumnName, ci.ColumnNames[i])

			val := ci.ColumnValues[i]

			switch cs.DataType {
			case string(schema.TypeInt64):
				require.IsType(t, int64(0), val)
			case string(schema.TypeInt32):
				require.IsType(t, int32(0), val)
			case string(schema.TypeInt16):
				require.IsType(t, int16(0), val)
			case string(schema.TypeInt8):
				require.IsType(t, int8(0), val)
			case string(schema.TypeUint64):
				require.IsType(t, uint64(0), val)
			case string(schema.TypeUint32):
				require.IsType(t, uint32(0), val)
			case string(schema.TypeUint16):
				require.IsType(t, uint16(0), val)
			case string(schema.TypeUint8):
				require.IsType(t, uint8(0), val)
			case string(schema.TypeFloat32):
				require.IsType(t, float32(0), val)
			case string(schema.TypeFloat64):
				require.IsType(t, float64(0), val)

			case string(schema.TypeBytes):
				require.IsType(t, []byte(nil), val)
			case string(schema.TypeString):
				require.IsType(t, string(""), val)
			case string(schema.TypeBoolean):
				require.IsType(t, false, val)
			case string(schema.TypeAny):
				_, isMap := val.(map[string]interface{})
				_, isArray := val.([]interface{})
				require.True(t, isMap || isArray, val)
			case string(schema.TypeDate):
				require.IsType(t, time.Time{}, val)
			case string(schema.TypeDatetime):
				require.IsType(t, time.Time{}, val)
			case string(schema.TypeTimestamp):
				require.IsType(t, time.Time{}, val)
			case string(schema.TypeInterval):
				require.IsType(t, time.Duration(0), val)
			default:
				require.FailNowf(t, "unknown data type: %s", cs.DataType)
			}
		}
	}
}

func getCloudExportHitsTestCases(t *testing.T) []struct {
	conf    ProtoParserConfig
	colsLen int
} {

	res := []struct {
		conf    ProtoParserConfig
		colsLen int
	}{
		{
			conf:    ProtoParserConfig{},
			colsLen: 92,
		},
		{
			conf: ProtoParserConfig{
				IncludeColumns: []ColParams{
					{
						Name:     "TransferID",
						Required: true,
					},
				},
				PrimaryKeys: []string{
					"TransferID",
					"WatchID",
				},
			},
			colsLen: 2,
		},
		{
			conf: ProtoParserConfig{
				IncludeColumns: []ColParams{
					{
						Name:     "TransferID",
						Required: true,
					},
				},
				PrimaryKeys: []string{
					"WatchID",
				},
			},
			colsLen: 2,
		},
		{
			conf: ProtoParserConfig{
				IncludeColumns: []ColParams{
					{
						Name:     "TransferID",
						Required: true,
					},
					{
						Name:     "WatchID",
						Required: true,
					},
				},
				PrimaryKeys: []string{
					"WatchID",
				},
			},
			colsLen: 2,
		},
		{
			conf: ProtoParserConfig{
				IncludeColumns: []ColParams{
					{
						Name:     "TransferID",
						Required: true,
					},
					{
						Name:     "WatchID",
						Required: true,
					},
				},
			},
			colsLen: 2,
		},
		{
			conf: ProtoParserConfig{
				AddSyntheticKeys: true,
			},
			colsLen: 95,
		},
		{
			conf: ProtoParserConfig{
				AddSyntheticKeys: true,
				AddSystemColumns: true,
			},
			colsLen: 97,
		},
		{
			conf: ProtoParserConfig{
				IncludeColumns: []ColParams{
					{
						Name:     "TransferID",
						Required: true,
					},
				},
				AddSyntheticKeys: true,
			},
			colsLen: 4,
		},
		{
			conf: ProtoParserConfig{
				IncludeColumns: []ColParams{
					{
						Name:     "TransferID",
						Required: true,
					},
				},
				PrimaryKeys: []string{
					"TransferID",
				},
				AddSyntheticKeys: true,
			},
			colsLen: 4,
		},
		{
			conf: ProtoParserConfig{
				IncludeColumns: []ColParams{
					{
						Name:     "TransferID",
						Required: true,
					},
				},
				PrimaryKeys: []string{
					"WatchID",
				},
				AddSyntheticKeys: true,
			},
			colsLen: 5,
		},
		{
			conf: ProtoParserConfig{
				PrimaryKeys: []string{
					"WatchID",
				},
				AddSyntheticKeys: true,
			},
			colsLen: 95,
		},
		{
			conf: ProtoParserConfig{
				IncludeColumns: []ColParams{
					{
						Name:     "TransferID",
						Required: true,
					},
				},
				AddSyntheticKeys: true,
				AddSystemColumns: true,
			},
			colsLen: 97,
		},
		{
			conf: ProtoParserConfig{
				IncludeColumns: []ColParams{
					{
						Name:     "TransferID",
						Required: true,
					},
				},
				PrimaryKeys: []string{
					"TransferID",
				},
				AddSyntheticKeys: true,
				AddSystemColumns: true,
			},
			colsLen: 97,
		},
		{
			conf: ProtoParserConfig{
				IncludeColumns: []ColParams{
					{
						Name:     "TransferID",
						Required: true,
					},
				},
				PrimaryKeys: []string{
					"TransferID",
					"WatchID",
				},
				AddSyntheticKeys: true,
				AddSystemColumns: true,
			},
			colsLen: 97,
		},
		{
			conf: ProtoParserConfig{
				PrimaryKeys: []string{
					"TransferID",
				},
				AddSyntheticKeys: true,
				AddSystemColumns: true,
			},
			colsLen: 97,
		},
	}

	rootMsgDesc, err := extractMessageDesc(
		protoSampleContent(t, "metrika_cloud_export_hit_log.desc"),
		"CloudTransferHitList",
	)
	require.NoError(t, err)

	embeddedMsgDesc, ok := extractEmbeddedRepeatedMsgDesc(rootMsgDesc)
	require.True(t, ok)

	for i := range res {
		res[i].conf.ScannerMessageDesc = rootMsgDesc
		res[i].conf.ProtoMessageDesc = embeddedMsgDesc
		res[i].conf.ProtoScannerType = protoscanner.ScannerTypeRepeated
		res[i].conf.LineSplitter = abstract.LfLineSplitterDoNotSplit
	}

	return res
}

func TestDoCloudExportHits(t *testing.T) {
	testCases := getCloudExportHitsTestCases(t)
	pMsg := parsers.Message{
		Offset:     1,
		SeqNo:      1,
		Key:        nil,
		CreateTime: time.Time{},
		WriteTime:  time.Time{},
		Value:      protoSampleContent(t, "metrika_cloud_export_hit_log_data.bin"),
		Headers:    nil,
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("case_%d", i), func(t *testing.T) {
			par, err := NewProtoParser(&tc.conf, getSourceStatsMock())
			require.NoError(t, err)

			got := par.Do(pMsg, abstract.NewPartition("", 0))

			unparsed := parsers.ExtractUnparsed(got)
			if len(unparsed) > 0 {
				require.FailNow(t, "unexpected unparsed items", unparsed)
			}

			checkColsEqual(t, got)

			for _, ci := range got {
				require.Equal(t, tc.colsLen, len(ci.ColumnNames))
			}
		})
	}
}

func TestDoStdDataTypesEqualValues(t *testing.T) {
	data, err := proto.Marshal(stdDataTypesFilled)
	require.NoError(t, err)

	pMsg := parsers.Message{
		Offset:     1,
		SeqNo:      1,
		Key:        nil,
		CreateTime: time.Time{},
		WriteTime:  time.Time{},
		Value:      data,
		Headers:    nil,
	}

	desc := stdDataTypesFilled.ProtoReflect().Descriptor()
	config := ProtoParserConfig{
		ProtoMessageDesc:   desc,
		ScannerMessageDesc: desc,
		ProtoScannerType:   protoscanner.ScannerTypeLineSplitter,
		LineSplitter:       abstract.LfLineSplitterDoNotSplit,
		IncludeColumns:     fieldList(desc, true),
	}

	par, err := NewProtoParser(&config, getSourceStatsMock())
	require.NoError(t, err)

	got := par.Do(pMsg, abstract.NewPartition("", 0))

	unparsed := parsers.ExtractUnparsed(got)
	if len(unparsed) > 0 {
		require.FailNow(t, "unexpected unparsed items", unparsed)
	}

	checkColsEqual(t, got)
	require.Equal(t, 1, len(got))
	checkEqualStdValues(t, got[0], stdDataTypesFilled.ProtoReflect())
}

// not deep equal: don't check embedded complicated types
func checkEqualStdValues(t *testing.T, ci abstract.ChangeItem, msg protoreflect.Message) {
	fields := msg.Descriptor().Fields()

	for i, ts := range ci.TableSchema.Columns() {
		fd := fields.ByTextName(ts.ColumnName)
		require.NotNil(t, fd, fd.TextName())

		require.True(t, msg.Has(fd), fd.TextName())

		val := msg.Get(fd)
		if fd.IsList() || fd.IsMap() || fd.Kind() == protoreflect.MessageKind {
			require.NotNil(t, val.Interface(), fd.TextName())
		} else {
			require.Equal(t, val.Interface(), ci.ColumnValues[i], fd.TextName())
		}
	}
}

func fieldList(desc protoreflect.MessageDescriptor, required bool) []ColParams {
	var res []ColParams

	for i := 0; i < desc.Fields().Len(); i++ {
		res = append(res, ColParams{
			Name:     desc.Fields().Get(i).TextName(),
			Required: required,
		})
	}

	return res
}

func makeRequiredSetTestCases() []struct {
	conf     ProtoParserConfig
	required []string
	keys     []string
} {
	res := []struct {
		conf     ProtoParserConfig
		required []string
		keys     []string
	}{
		{
			conf:     ProtoParserConfig{},
			required: []string{},
			keys:     []string{},
		},
		{
			conf: ProtoParserConfig{
				PrimaryKeys: []string{"doubleField", "floatField"},
			},
			required: []string{"doubleField", "floatField"},
			keys:     []string{"doubleField", "floatField"},
		},
		{
			conf: ProtoParserConfig{
				PrimaryKeys:     []string{"doubleField", "floatField"},
				NullKeysAllowed: true,
			},
			required: []string{},
			keys:     []string{"doubleField", "floatField"},
		},
		{
			conf: ProtoParserConfig{
				PrimaryKeys: []string{"doubleField", "floatField"},
				IncludeColumns: []ColParams{
					{
						Name:     "doubleField",
						Required: true,
					},
				},
				NullKeysAllowed: true,
			},
			required: []string{"doubleField"},
			keys:     []string{"doubleField", "floatField"},
		},
		{
			conf: ProtoParserConfig{
				PrimaryKeys: []string{"doubleField", "floatField"},
				IncludeColumns: []ColParams{
					{
						Name:     "doubleField",
						Required: false,
					},
				},
				NullKeysAllowed: true,
			},
			required: []string{},
			keys:     []string{"doubleField", "floatField"},
		},
		{
			conf: ProtoParserConfig{
				PrimaryKeys: []string{"doubleField"},
				IncludeColumns: []ColParams{
					{
						Name:     "floatField",
						Required: false,
					},
					{
						Name:     "doubleField",
						Required: false,
					},
				},
				NullKeysAllowed: true,
			},
			required: []string{},
			keys:     []string{"doubleField"},
		},
		{
			conf: ProtoParserConfig{
				PrimaryKeys: []string{"doubleField"},
				IncludeColumns: []ColParams{
					{
						Name:     "floatField",
						Required: true,
					},
					{
						Name:     "doubleField",
						Required: false,
					},
				},
				NullKeysAllowed: true,
			},
			required: []string{"floatField"},
			keys:     []string{"doubleField"},
		},
		{
			conf: ProtoParserConfig{
				PrimaryKeys: []string{"doubleField"},
				IncludeColumns: []ColParams{
					{
						Name:     "floatField",
						Required: true,
					},
					{
						Name:     "doubleField",
						Required: true,
					},
				},
			},
			required: []string{"floatField", "doubleField"},
			keys:     []string{"doubleField"},
		},
		{
			conf: ProtoParserConfig{
				PrimaryKeys:      []string{"doubleField"},
				AddSyntheticKeys: true,
				NullKeysAllowed:  true,
			},
			required: []string{},
			keys:     []string{"doubleField", parsers.SyntheticIdxCol, parsers.SyntheticOffsetCol, parsers.SyntheticPartitionCol},
		},
		{
			conf: ProtoParserConfig{
				PrimaryKeys:      []string{"doubleField"},
				AddSyntheticKeys: true,
			},
			required: []string{"doubleField", parsers.SyntheticIdxCol, parsers.SyntheticOffsetCol, parsers.SyntheticPartitionCol},
			keys:     []string{"doubleField", parsers.SyntheticIdxCol, parsers.SyntheticOffsetCol, parsers.SyntheticPartitionCol},
		},
		{
			//10
			conf: ProtoParserConfig{
				PrimaryKeys: []string{"doubleField"},
				IncludeColumns: []ColParams{
					{
						Name:     "floatField",
						Required: true,
					},
				},
				AddSyntheticKeys: true,
			},
			required: []string{"doubleField", "floatField", parsers.SyntheticIdxCol, parsers.SyntheticOffsetCol, parsers.SyntheticPartitionCol},
			keys:     []string{"doubleField", parsers.SyntheticIdxCol, parsers.SyntheticOffsetCol, parsers.SyntheticPartitionCol},
		},
		{
			conf: ProtoParserConfig{
				PrimaryKeys: []string{"doubleField"},
				IncludeColumns: []ColParams{
					{
						Name:     "floatField",
						Required: true,
					},
				},
				AddSyntheticKeys: true,
				AddSystemColumns: true,
				SkipDedupKeys:    true,
			},
			required: []string{"doubleField", "floatField", parsers.SyntheticIdxCol, parsers.SyntheticOffsetCol, parsers.SyntheticPartitionCol},
			keys:     []string{"doubleField", parsers.SyntheticIdxCol, parsers.SyntheticOffsetCol, parsers.SyntheticPartitionCol},
		},
		{
			conf: ProtoParserConfig{
				PrimaryKeys: []string{"doubleField"},
				IncludeColumns: []ColParams{
					{
						Name:     "floatField",
						Required: true,
					},
				},
				AddSyntheticKeys: true,
				AddSystemColumns: true,
				SkipDedupKeys:    false,
			},
			required: []string{"doubleField", "floatField", parsers.SyntheticIdxCol, parsers.SyntheticOffsetCol, parsers.SyntheticPartitionCol, parsers.SystemLbCtimeCol, parsers.SystemLbWtimeCol},
			keys:     []string{"doubleField", parsers.SyntheticIdxCol, parsers.SyntheticOffsetCol, parsers.SyntheticPartitionCol, parsers.SystemLbCtimeCol, parsers.SystemLbWtimeCol},
		},
		{
			conf: ProtoParserConfig{
				PrimaryKeys: []string{"doubleField"},
				IncludeColumns: []ColParams{
					{
						Name:     "floatField",
						Required: true,
					},
				},
				AddSyntheticKeys: true,
				AddSystemColumns: true,
				SkipDedupKeys:    false,
				NullKeysAllowed:  true,
			},
			required: []string{"floatField"},
			keys:     []string{"doubleField", parsers.SyntheticIdxCol, parsers.SyntheticOffsetCol, parsers.SyntheticPartitionCol, parsers.SystemLbCtimeCol, parsers.SystemLbWtimeCol},
		},
	}

	desc := stdDataTypesFilled.ProtoReflect().Descriptor()

	for i := range res {
		res[i].conf.ProtoMessageDesc = desc
		res[i].conf.ScannerMessageDesc = desc
		res[i].conf.ProtoScannerType = protoscanner.ScannerTypeLineSplitter
		res[i].conf.LineSplitter = abstract.LfLineSplitterDoNotSplit
	}

	return res
}

func TestDoStdDataTypesRequiredAndPKSet(t *testing.T) {
	data, err := proto.Marshal(stdDataTypesFilled)
	require.NoError(t, err)

	pMsg := parsers.Message{
		Offset:     1,
		SeqNo:      1,
		Key:        nil,
		CreateTime: time.Time{},
		WriteTime:  time.Time{},
		Value:      data,
		Headers:    nil,
	}

	cases := makeRequiredSetTestCases()

	for i, tc := range cases {
		t.Run(fmt.Sprintf("case_%d", i), func(t *testing.T) {
			par, err := NewProtoParser(&tc.conf, getSourceStatsMock())
			require.NoError(t, err)

			got := par.Do(pMsg, abstract.NewPartition("", 0))

			unparsed := parsers.ExtractUnparsed(got)
			if len(unparsed) > 0 {
				require.FailNow(t, "unexpected unparsed items", unparsed)
			}

			checkColsEqual(t, got)
			require.Equal(t, 1, len(got))

			// check Required
			checkEqualFieldsFunc(t, got, tc.required, func(cs abstract.ColSchema) bool { return cs.Required })
			// check PK
			checkEqualFieldsFunc(t, got, tc.keys, func(cs abstract.ColSchema) bool { return cs.PrimaryKey })
		})
	}
}

func checkEqualFieldsFunc(t *testing.T, items []abstract.ChangeItem, expFields []string, cond func(abstract.ColSchema) bool) {
	exp := make(map[string]bool)
	for _, name := range expFields {
		exp[name] = true
	}

	for _, ci := range items {
		got := make(map[string]bool)

		for _, cs := range ci.TableSchema.Columns() {
			if cond(cs) {
				got[cs.ColumnName] = true
			}
		}

		require.Equal(t, exp, got)
	}
}

func makeRequiredNotSetTestCases() []ProtoParserConfig {
	res := []ProtoParserConfig{
		{
			PrimaryKeys: []string{"msgField"},
		},
	}

	desc := stdDataTypesEmpty.ProtoReflect().Descriptor()

	for i := range res {
		res[i].ProtoMessageDesc = desc
		res[i].ScannerMessageDesc = desc
		res[i].ProtoScannerType = protoscanner.ScannerTypeLineSplitter
		res[i].LineSplitter = abstract.LfLineSplitterDoNotSplit
	}

	return res
}

func TestDoRequiredNotSet(t *testing.T) {
	data, err := proto.Marshal(stdDataTypesEmpty)
	require.NoError(t, err)

	pMsg := parsers.Message{
		Offset:     1,
		SeqNo:      1,
		Key:        nil,
		CreateTime: time.Time{},
		WriteTime:  time.Time{},
		Value:      data,
		Headers:    nil,
	}

	cases := makeRequiredNotSetTestCases()

	for i, tc := range cases {
		t.Run(fmt.Sprintf("case_%d", i), func(t *testing.T) {
			par, err := NewProtoParser(&tc, getSourceStatsMock())
			require.NoError(t, err)

			got := par.Do(pMsg, abstract.NewPartition("", 0))

			unparsed := parsers.ExtractUnparsed(got)
			if len(unparsed) != 1 {
				require.FailNow(t, "expected exactly one unparsed items, but got", got)
			}
		})
	}
}
