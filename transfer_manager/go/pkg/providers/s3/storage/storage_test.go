package storage

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/file"
	"github.com/apache/arrow/go/v13/parquet/schema"
	"github.com/doublecloud/tross/library/go/core/metrics/solomon"
	"github.com/doublecloud/tross/library/go/test/canon"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/predicate"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/s3"
	"github.com/stretchr/testify/require"
)

func TestArrow(t *testing.T) {
	rdr, err := file.OpenParquetFile("./yellow_taxi/demo.parquet", true)
	require.NoError(t, err)
	require.NotNil(t, rdr)

	selectedColumns := []int{}
	fileMetadata := rdr.MetaData()
	for i := 0; i < fileMetadata.Schema.NumColumns(); i++ {
		selectedColumns = append(selectedColumns, i)
	}
	fileMetadata.Size()
	blockSize := 10 * 1024

	st := time.Now()
	cis := make([]abstract.ChangeItem, 0, blockSize)
	for r := 0; r < rdr.NumRowGroups(); r++ {
		rgr := rdr.RowGroup(r)
		logger.Log.Infof("row group size: %v", rgr.NumRows())
		scanners := make([]*Dumper, len(selectedColumns))
		fields := make([]string, len(selectedColumns))
		for idx, c := range selectedColumns {
			col, err := rgr.Column(c)
			require.NoError(t, err)
			scanners[idx] = createDumper(col)
			fields[idx] = col.Descriptor().Path()
		}

	GROUP:
		for {
			var ci abstract.ChangeItem
			ci.Kind = abstract.InsertKind
			ci.ColumnValues = make([]interface{}, len(fields))
			ci.ColumnNames = fields
			ci.Table = "demo"
			for idx, s := range scanners {
				if val, ok := s.Next(); ok {
					if val == nil {
						continue
					}
					ci.ColumnValues[idx] = s.FormatValue(val, 0)
				} else {
					break GROUP
				}
			}
			cis = append(cis, ci)
			if len(cis) == blockSize {
				logger.Log.Infof("read %v rows", len(cis))
				cis = make([]abstract.ChangeItem, 0, blockSize)
			}
		}
		if len(cis) > 0 {
			logger.Log.Infof("group read done, left %v rows", len(cis))
			cis = make([]abstract.ChangeItem, 0, blockSize)
		}
	}
	logger.Log.Infof("dummy load in: %v", time.Since(st))
}

const defaultBatchSize = 128

type Dumper struct {
	reader         file.ColumnChunkReader
	batchSize      int64
	valueOffset    int
	valuesBuffered int

	levelOffset    int64
	levelsBuffered int64
	defLevels      []int16
	repLevels      []int16

	valueBuffer interface{}
}

func createDumper(reader file.ColumnChunkReader) *Dumper {
	batchSize := defaultBatchSize

	var valueBuffer interface{}
	switch reader.(type) {
	case *file.BooleanColumnChunkReader:
		valueBuffer = make([]bool, batchSize)
	case *file.Int32ColumnChunkReader:
		valueBuffer = make([]int32, batchSize)
	case *file.Int64ColumnChunkReader:
		valueBuffer = make([]int64, batchSize)
	case *file.Float32ColumnChunkReader:
		valueBuffer = make([]float32, batchSize)
	case *file.Float64ColumnChunkReader:
		valueBuffer = make([]float64, batchSize)
	case *file.Int96ColumnChunkReader:
		valueBuffer = make([]parquet.Int96, batchSize)
	case *file.ByteArrayColumnChunkReader:
		valueBuffer = make([]parquet.ByteArray, batchSize)
	case *file.FixedLenByteArrayColumnChunkReader:
		valueBuffer = make([]parquet.FixedLenByteArray, batchSize)
	}

	return &Dumper{
		reader:      reader,
		batchSize:   int64(batchSize),
		defLevels:   make([]int16, batchSize),
		repLevels:   make([]int16, batchSize),
		valueBuffer: valueBuffer,
	}
}

func (dump *Dumper) readNextBatch() {
	switch reader := dump.reader.(type) {
	case *file.BooleanColumnChunkReader:
		values := dump.valueBuffer.([]bool)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.Int32ColumnChunkReader:
		values := dump.valueBuffer.([]int32)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.Int64ColumnChunkReader:
		values := dump.valueBuffer.([]int64)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.Float32ColumnChunkReader:
		values := dump.valueBuffer.([]float32)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.Float64ColumnChunkReader:
		values := dump.valueBuffer.([]float64)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.Int96ColumnChunkReader:
		values := dump.valueBuffer.([]parquet.Int96)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.ByteArrayColumnChunkReader:
		values := dump.valueBuffer.([]parquet.ByteArray)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.FixedLenByteArrayColumnChunkReader:
		values := dump.valueBuffer.([]parquet.FixedLenByteArray)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	}

	dump.valueOffset = 0
	dump.levelOffset = 0
}

func (dump *Dumper) hasNext() bool {
	return dump.levelOffset < dump.levelsBuffered || dump.reader.HasNext()
}

func (dump *Dumper) Next() (interface{}, bool) {
	if dump.levelOffset == dump.levelsBuffered {
		if !dump.hasNext() {
			return nil, false
		}
		dump.readNextBatch()
		if dump.levelsBuffered == 0 {
			return nil, false
		}
	}

	defLevel := dump.defLevels[int(dump.levelOffset)]
	// repLevel := dump.repLevels[int(dump.levelOffset)]
	dump.levelOffset++

	if defLevel < dump.reader.Descriptor().MaxDefinitionLevel() {
		return nil, true
	}

	vb := reflect.ValueOf(dump.valueBuffer)
	v := vb.Index(dump.valueOffset).Interface()
	dump.valueOffset++

	return v, true
}

const microSecondsPerDay = 24 * 3600e6

func (dump *Dumper) FormatValue(val interface{}, width int) string {
	fmtstring := fmt.Sprintf("-%d", width)
	switch val := val.(type) {
	case nil:
		return fmt.Sprintf("%"+fmtstring+"s", "NULL")
	case bool:
		return fmt.Sprintf("%"+fmtstring+"t", val)
	case int32:
		return fmt.Sprintf("%"+fmtstring+"d", val)
	case int64:
		return fmt.Sprintf("%"+fmtstring+"d", val)
	case float32:
		return fmt.Sprintf("%"+fmtstring+"f", val)
	case float64:
		return fmt.Sprintf("%"+fmtstring+"f", val)
	case parquet.Int96:
		usec := int64(binary.LittleEndian.Uint64(val[:8])/1000) +
			(int64(binary.LittleEndian.Uint32(val[8:]))-2440588)*microSecondsPerDay
		t := time.Unix(usec/1e6, (usec%1e6)*1e3).UTC()
		return fmt.Sprintf("%"+fmtstring+"s", t)

	case parquet.ByteArray:
		if dump.reader.Descriptor().ConvertedType() == schema.ConvertedTypes.UTF8 {
			return fmt.Sprintf("%"+fmtstring+"s", string(val))
		}
		return fmt.Sprintf("% "+fmtstring+"X", val)
	case parquet.FixedLenByteArray:
		return fmt.Sprintf("% "+fmtstring+"X", val)
	default:
		return fmt.Sprintf("%"+fmtstring+"s", fmt.Sprintf("%v", val))
	}
}

func TestCanonParquet(t *testing.T) {
	testCasePath := "yellow_taxi"
	cfg := s3.PrepareCfg(t, "data3", "")
	cfg.PathPrefix = testCasePath
	if os.Getenv("S3MDS_PORT") != "" { // for local recipe we need to upload test case to internet
		s3.PrepareTestCase(t, cfg, cfg.PathPrefix)
		logger.Log.Info("dir uploaded")
	}
	cfg.ReadBatchSize = 100_000
	storage, err := New(cfg, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)
	schema, err := storage.TableList(nil)
	require.NoError(t, err)
	tid := *abstract.NewTableID("test_namespace", "test_name")
	for _, col := range schema[tid].Schema.Columns() {
		logger.Log.Infof("resolved schema: %s (%s) %v", col.ColumnName, col.DataType, col.PrimaryKey)
	}
	totalRows, err := storage.ExactTableRowsCount(tid)
	require.NoError(t, err)
	logger.Log.Infof("estimate %v rows", totalRows)
	require.Equal(t, 12554664, int(totalRows))
	tdesc, err := storage.ShardTable(context.Background(), abstract.TableDescription{Name: tid.Name, Schema: tid.Namespace})
	require.NoError(t, err)
	require.Equal(t, len(tdesc), 4)
	wg := sync.WaitGroup{}
	wg.Add(len(tdesc))
	cntr := &atomic.Int64{}
	fileSnippets := map[abstract.TableDescription]abstract.TypedChangeItem{}
	for _, desc := range tdesc {
		go func(desc abstract.TableDescription) {
			defer wg.Done()
			require.NoError(
				t,
				storage.LoadTable(context.Background(), desc, func(items []abstract.ChangeItem) error {
					if _, ok := fileSnippets[desc]; !ok {
						fileSnippets[desc] = abstract.TypedChangeItem(items[0])
					}
					logger.Log.Infof("pushed: \n%s", abstract.Sniff(items))
					_ = cntr.Add(int64(len(items)))
					return nil
				}),
			)
		}(desc)
	}
	wg.Wait()
	require.Equal(t, int(totalRows), int(cntr.Load()))

	var totalCanon []string
	for desc, sample := range fileSnippets {
		sample.CommitTime = 0
		rawJSON, err := json.MarshalIndent(&sample, "", "    ")
		require.NoError(t, err)
		operands, err := predicate.InclusionOperands(desc.Filter, s3FileNameCol)
		require.NoError(t, err)
		require.Len(t, operands, 1)

		canonData := fmt.Sprintf("file: %s\n%s", operands[0].Val, string(rawJSON))
		require.NoError(t, err)
		fmt.Println(canonData)
		totalCanon = append(totalCanon, canonData)
	}
	sort.Strings(totalCanon)
	canon.SaveJSON(t, strings.Join(totalCanon, "\n"))
}

func TestCanonJsonline(t *testing.T) {
	testCasePath := "test_jsonline_files"
	cfg := s3.PrepareCfg(t, "jsonline_canon", server.ParsingFormatJSONLine)
	cfg.PathPrefix = testCasePath
	if os.Getenv("S3MDS_PORT") != "" { // for local recipe we need to upload test case to internet
		s3.PrepareTestCase(t, cfg, cfg.PathPrefix)
		logger.Log.Info("dir uploaded")
	}
	cfg.ReadBatchSize = 100_000
	cfg.Format.JSONLSetting = new(s3.JSONLSetting)
	cfg.Format.JSONLSetting.BlockSize = 100_000
	storage, err := New(cfg, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)
	schema, err := storage.TableList(nil)
	require.NoError(t, err)
	tid := *abstract.NewTableID("test_namespace", "test_name")
	for _, col := range schema[tid].Schema.Columns() {
		logger.Log.Infof("resolved schema: %s (%s) %v", col.ColumnName, col.DataType, col.PrimaryKey)
	}

	tdesc, err := storage.ShardTable(context.Background(), abstract.TableDescription{Name: tid.Name, Schema: tid.Namespace})
	require.NoError(t, err)
	wg := sync.WaitGroup{}
	wg.Add(len(tdesc))
	cntr := &atomic.Int64{}
	fileSnippets := map[abstract.TableDescription]abstract.TypedChangeItem{}
	for _, desc := range tdesc {
		go func(desc abstract.TableDescription) {
			defer wg.Done()
			require.NoError(
				t,
				storage.LoadTable(context.Background(), desc, func(items []abstract.ChangeItem) error {
					if _, ok := fileSnippets[desc]; !ok {
						fileSnippets[desc] = abstract.TypedChangeItem(items[0])
					}
					logger.Log.Infof("pushed: \n%s", abstract.Sniff(items))
					_ = cntr.Add(int64(len(items)))
					return nil
				}),
			)
		}(desc)
	}
	wg.Wait()

	var totalCanon []string
	for desc, sample := range fileSnippets {
		sample.CommitTime = 0
		rawJSON, err := json.MarshalIndent(&sample, "", "    ")
		require.NoError(t, err)
		operands, err := predicate.InclusionOperands(desc.Filter, s3FileNameCol)
		require.NoError(t, err)
		require.Len(t, operands, 1)

		canonData := fmt.Sprintf("file: %s\n%s", operands[0].Val, string(rawJSON))
		require.NoError(t, err)
		fmt.Println(canonData)
		totalCanon = append(totalCanon, canonData)
	}
	sort.Strings(totalCanon)
	canon.SaveJSON(t, strings.Join(totalCanon, "\n"))
}

func TestCanonCsv(t *testing.T) {
	testCasePath := "test_csv_large"
	cfg := s3.PrepareCfg(t, "csv_canon", server.ParsingFormatCSV)
	cfg.PathPrefix = testCasePath
	if os.Getenv("S3MDS_PORT") != "" { // for local recipe we need to upload test case to internet
		s3.PrepareTestCase(t, cfg, cfg.PathPrefix)
		logger.Log.Info("dir uploaded")
	}
	cfg.ReadBatchSize = 100_000_0
	cfg.Format.CSVSetting = new(s3.CSVSetting)
	cfg.Format.CSVSetting.BlockSize = 100_000_0
	cfg.Format.CSVSetting.Delimiter = ","
	cfg.Format.CSVSetting.QuoteChar = "\""
	cfg.Format.CSVSetting.EscapeChar = "\\"
	storage, err := New(cfg, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)
	schema, err := storage.TableList(nil)
	require.NoError(t, err)
	tid := *abstract.NewTableID("test_namespace", "test_name")
	for _, col := range schema[tid].Schema.Columns() {
		logger.Log.Infof("resolved schema: %s (%s) %v", col.ColumnName, col.DataType, col.PrimaryKey)
	}

	tdesc, err := storage.ShardTable(context.Background(), abstract.TableDescription{Name: tid.Name, Schema: tid.Namespace})
	require.NoError(t, err)
	wg := sync.WaitGroup{}
	wg.Add(len(tdesc))
	cntr := &atomic.Int64{}
	fileSnippets := map[abstract.TableDescription]abstract.TypedChangeItem{}
	for _, desc := range tdesc {
		go func(desc abstract.TableDescription) {
			defer wg.Done()
			require.NoError(
				t,
				storage.LoadTable(context.Background(), desc, func(items []abstract.ChangeItem) error {
					if _, ok := fileSnippets[desc]; !ok {
						fileSnippets[desc] = abstract.TypedChangeItem(items[0])
					}
					logger.Log.Infof("pushed: \n%s", abstract.Sniff(items))
					_ = cntr.Add(int64(len(items)))
					return nil
				}),
			)
		}(desc)
	}
	wg.Wait()

	rows, err := storage.EstimateTableRowsCount(tid)
	require.NoError(t, err)
	diff := math.Abs(float64(cntr.Load()) - float64(rows))
	percent := (diff * 100) / float64(cntr.Load())
	// check that row estimation is at most 5 % off
	require.Less(t, percent, float64(5))

	require.Equal(t, int(500000), int(cntr.Load()))

	var totalCanon []string
	for desc, sample := range fileSnippets {
		sample.CommitTime = 0
		rawJSON, err := json.MarshalIndent(&sample, "", "    ")
		canonData := fmt.Sprintf("file: %s\n%s", desc.Filter, string(rawJSON))
		require.NoError(t, err)
		fmt.Println(canonData)
		totalCanon = append(totalCanon, canonData)
	}
	sort.Strings(totalCanon)
	canon.SaveJSON(t, strings.Join(totalCanon, "\n"))
}

func TestEstimateTableRowsCount(t *testing.T) {
	testCasePath := "test_csv_large"
	cfg := s3.PrepareCfg(t, "estimate_rows", server.ParsingFormatCSV)
	cfg.PathPrefix = testCasePath
	if os.Getenv("S3MDS_PORT") != "" { // for local recipe we need to upload test case to internet
		s3.PrepareTestCase(t, cfg, cfg.PathPrefix)
		logger.Log.Info("dir uploaded")
	}
	cfg.ReadBatchSize = 100_000_0
	cfg.Format.CSVSetting = new(s3.CSVSetting)
	cfg.Format.CSVSetting.BlockSize = 100_000_0
	cfg.Format.CSVSetting.Delimiter = ","
	cfg.Format.CSVSetting.QuoteChar = "\""
	cfg.Format.CSVSetting.EscapeChar = "\\"
	cfg.EventSource.SQS = &s3.SQS{
		QueueName: "test",
	}

	storage, err := New(cfg, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	zeroRes, err := storage.EstimateTableRowsCount(*abstract.NewTableID("test", "name"))
	require.NoError(t, err)
	require.Equal(t, uint64(0), zeroRes) // nothing estimated since eventSource configured

	cfg.EventSource.SQS = nil

	res, err := storage.EstimateTableRowsCount(*abstract.NewTableID("test", "name"))
	require.NoError(t, err)
	require.Equal(t, uint64(508060), res) // actual estimated row size
}
