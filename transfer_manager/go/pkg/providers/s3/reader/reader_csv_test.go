package reader

import (
	"bufio"
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/doublecloud/tross/library/go/core/metrics/solomon"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/csv"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/s3"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/stats"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/schema"
)

var (
	//go:embed gotest/dumb/small.csv
	smallCSV []byte
)

func TestResolveCSVSchema(t *testing.T) {
	src := s3.PrepareCfg(t, "data4", "")

	if os.Getenv("S3MDS_PORT") != "" {
		// for local recipe we need to upload test case to internet
		src.PathPrefix = "test_csv_schemas"
		s3.PrepareTestCase(t, src, src.PathPrefix)
		logger.Log.Info("dir uploaded")
	}

	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(src.ConnectionConfig.Endpoint),
		Region:           aws.String(src.ConnectionConfig.Region),
		S3ForcePathStyle: aws.Bool(src.ConnectionConfig.S3ForcePathStyle),
		Credentials: credentials.NewStaticCredentials(
			src.ConnectionConfig.AccessKey, string(src.ConnectionConfig.SecretKey), "",
		),
	})

	require.NoError(t, err)

	csvReader := CSVReader{
		client:          aws_s3.New(sess),
		pathPrefix:      "test_csv_schemas",
		batchSize:       1 * 1024 * 1024,
		blockSize:       1 * 1024 * 1024,
		logger:          logger.Log,
		bucket:          src.Bucket,
		delimiter:       ',',
		quoteChar:       '"',
		doubleQuote:     true,
		newlinesInValue: true,
		escapeChar:      '\\',
		metrics:         stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts())),
	}

	res, err := csvReader.ResolveSchema(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, res.Columns())

	t.Run("preexisting table schema", func(t *testing.T) {
		csvReader.tableSchema = abstract.NewTableSchema([]abstract.ColSchema{
			{
				TableSchema: "test-schema",
				TableName:   "test-name",
				ColumnName:  "test-1",
				PrimaryKey:  false,
			}, {
				TableSchema: "test-schema",
				TableName:   "test-name",
				ColumnName:  "test-2",
				PrimaryKey:  true,
			},
		})

		expectedSchema, err := csvReader.ResolveSchema(context.Background())
		require.NoError(t, err)
		require.Equal(t, 2, len(expectedSchema.Columns()))
		require.Equal(t, csvReader.tableSchema, expectedSchema)
	})

	t.Run("first line header schema", func(t *testing.T) {
		schema, err := csvReader.resolveSchema(context.Background(), "test_csv_schemas/simple.csv")
		require.NoError(t, err)
		require.Equal(t, []string{"name", "surname", "st.", "city", "state", "zip-code"}, schema.Columns().ColumnNames())
		require.Equal(t, []string{"utf8", "utf8", "utf8", "utf8", "utf8", "double"}, dataTypes(schema.Columns()))
	})

	t.Run("autogenerate schema", func(t *testing.T) {
		csvReader.advancedOptions.AutogenerateColumnNames = true
		schema, err := csvReader.resolveSchema(context.Background(), "test_csv_schemas/no_header.csv")
		require.NoError(t, err)
		require.Equal(t, []string{"f0", "f1", "f2", "f3", "f4", "f5"}, schema.Columns().ColumnNames())
		require.Equal(t, []string{"utf8", "utf8", "utf8", "utf8", "utf8", "double"}, dataTypes(schema.Columns()))
	})

	t.Run("extract schema", func(t *testing.T) {
		csvReader.advancedOptions.ColumnNames = []string{"name", "surname", "st.", "city", "state", "zip-code"}
		schema, err := csvReader.resolveSchema(context.Background(), "test_csv_schemas/no_header.csv")
		require.NoError(t, err)
		require.Equal(t, []string{"name", "surname", "st.", "city", "state", "zip-code"}, schema.Columns().ColumnNames())
		require.Equal(t, []string{"utf8", "utf8", "utf8", "utf8", "utf8", "double"}, dataTypes(schema.Columns()))
	})
}

func TestConstructCI(t *testing.T) {
	csvReader := CSVReader{
		logger:  logger.Log,
		metrics: stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts())),
	}

	csvReader.tableSchema = abstract.NewTableSchema([]abstract.ColSchema{
		{
			TableSchema: "test-schema",
			TableName:   "test-name",
			ColumnName:  "test-first-column",
			DataType:    schema.TypeBoolean.String(),
			PrimaryKey:  false,
			Path:        "0",
		},
		{
			TableSchema: "test-schema",
			TableName:   "test-name",
			ColumnName:  "test-missing-row-column",
			DataType:    schema.TypeString.String(),
			PrimaryKey:  false,
			Path:        "1",
		},
	})

	t.Run("missing cols are included", func(t *testing.T) {
		row := []string{"true"} // only one element in row from csv but 2 cols in schema
		csvReader.additionalReaderOptions.IncludeMissingColumns = true
		ci, err := csvReader.constructCI(row, "test_file", time.Now(), 1)
		require.NoError(t, err)
		require.Len(t, ci.ColumnValues, 2)
		require.Equal(t, []interface{}{true, ""}, ci.ColumnValues)
	})

	t.Run("missing cols flag is disabled", func(t *testing.T) {
		csvReader.additionalReaderOptions.IncludeMissingColumns = false
		row := []string{"true"} // only one element in row from csv
		_, err := csvReader.constructCI(row, "test_file", time.Now(), 1)
		require.Error(t, err)
		require.ErrorContains(t, err, "missing row element for column: test-missing-row-column, row elements: 1, columns: 2")
	})

	t.Run("missing cols flag is disabled but all elements present", func(t *testing.T) {
		csvReader.additionalReaderOptions.IncludeMissingColumns = false
		row := []string{"true", "this is a test string"} // 2 elements in row from csv for 2 cols
		ci, err := csvReader.constructCI(row, "test_file", time.Now(), 1)
		require.NoError(t, err)
		require.Len(t, ci.ColumnValues, 2)
		require.Equal(t, []interface{}{true, "this is a test string"}, ci.ColumnValues)
	})

	t.Run("schema contains sys cols", func(t *testing.T) {
		csvReader.additionalReaderOptions.IncludeMissingColumns = false
		csvReader.tableSchema = appendSystemColsTableSchema(csvReader.tableSchema.Columns())
		row := []string{"true", "this is a test string"} // 2 elements in row from csv for 4 cols, but 2 are sys cols
		ci, err := csvReader.constructCI(row, "test_file", time.Now(), 1)
		require.NoError(t, err)
		require.Len(t, ci.ColumnValues, 4) // we expect 4 values 2 that we read and 32 from the sys cols
		require.Equal(t, []interface{}{"test_file", uint64(1), true, "this is a test string"}, ci.ColumnValues)
	})
}

func BenchmarkCSVRead(b *testing.B) {
	r := CSVReader{
		logger:          logger.Log,
		metrics:         stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts())),
		newlinesInValue: true,
		quoteChar:       '"',
		escapeChar:      '"',
		doubleQuote:     true,
		blockSize:       10000000,
		delimiter:       ',',
		headerPresent:   true,
		batchSize:       100,
		advancedOptions: s3.AdvancedOptions{
			SkipRows:                1,
			SkipRowsAfterNames:      1,
			ColumnNames:             nil,
			AutogenerateColumnNames: false,
		},
	}

	csvReader := csv.NewReader(bufio.NewReaderSize(bytes.NewReader(smallCSV), 1024))
	csvReader.NewlinesInValue = r.newlinesInValue
	csvReader.QuoteChar = r.quoteChar
	csvReader.EscapeChar = r.escapeChar
	csvReader.Encoding = r.encoding
	csvReader.Delimiter = r.delimiter
	csvReader.DoubleQuote = r.doubleQuote
	csvReader.DoubleQuoteStr = fmt.Sprintf("%s%s", string(r.quoteChar), string(r.quoteChar))
	allColNames, err := r.getColumnNames(csvReader)
	require.NoError(b, err)
	filteredColNames, err := r.filterColNames(allColNames)
	require.NoError(b, err)
	s, err := r.getColumnTypes(filteredColNames, csvReader)
	require.NoError(b, err)

	r.tableSchema = abstract.NewTableSchema(s)
	r.fastCols = r.tableSchema.FastColumns()
	r.colNames = r.tableSchema.ColumnNames()

	for n := 0; n < b.N; n++ {
		csvReader = csv.NewReader(bytes.NewReader(smallCSV))
		csvReader.NewlinesInValue = r.newlinesInValue
		csvReader.QuoteChar = r.quoteChar
		csvReader.EscapeChar = r.escapeChar
		csvReader.Encoding = r.encoding
		csvReader.Delimiter = r.delimiter
		csvReader.DoubleQuote = r.doubleQuote
		csvReader.DoubleQuoteStr = fmt.Sprintf("%s%s", string(r.quoteChar), string(r.quoteChar))
		csvReader.ExpectedCols = r.colNames
		l := uint64(0)
		buf, err := r.ParseCSVRows(csvReader, "test", time.Now(), &l)
		require.NoError(b, err)
		require.NotEmpty(b, buf)
		totalS := 0
		totalC := 0
		for _, r := range buf {
			totalS += int(r.Size.Read)
			totalC += len(r.ColumnValues)
		}
		b.SetBytes(int64(totalS))
		require.True(b, totalC > 0)
	}
	b.ReportAllocs()
}

func TestReadLargeCSV(t *testing.T) {
	r := CSVReader{
		logger:          logger.Log,
		metrics:         stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts())),
		newlinesInValue: true,
		quoteChar:       '"',
		escapeChar:      '"',
		doubleQuote:     true,
		blockSize:       10000000,
		delimiter:       ',',
		headerPresent:   true,
		batchSize:       1024,
		advancedOptions: s3.AdvancedOptions{
			SkipRows:                1,
			SkipRowsAfterNames:      1,
			ColumnNames:             nil,
			AutogenerateColumnNames: false,
		},
	}

	fileData, err := os.ReadFile("demo.csv")
	require.NoError(t, err)
	logger.Log.Infof("begin reader")
	csvReader := csv.NewReader(bufio.NewReaderSize(bytes.NewReader(fileData), 1024))
	csvReader.NewlinesInValue = r.newlinesInValue
	csvReader.QuoteChar = r.quoteChar
	csvReader.EscapeChar = r.escapeChar
	csvReader.Encoding = r.encoding
	csvReader.Delimiter = r.delimiter
	csvReader.DoubleQuote = r.doubleQuote
	csvReader.DoubleQuoteStr = fmt.Sprintf("%s%s", string(r.quoteChar), string(r.quoteChar))
	allColNames, err := r.getColumnNames(csvReader)
	require.NoError(t, err)
	filteredColNames, err := r.filterColNames(allColNames)
	require.NoError(t, err)
	s, err := r.getColumnTypes(filteredColNames, csvReader)
	require.NoError(t, err)
	r.tableSchema = abstract.NewTableSchema(s)
	r.fastCols = r.tableSchema.FastColumns()
	r.colNames = r.tableSchema.ColumnNames()
	l := uint64(0)
	buf, err := r.ParseCSVRows(csvReader, "test", time.Now(), &l)
	require.NoError(t, err)
	require.NotEmpty(t, buf)
	totalS := 0
	totalC := 0
	for _, r := range buf {
		totalS += int(r.Size.Read)
		totalC += len(r.ColumnValues)
	}
	for {
		buff, err := r.ParseCSVRows(csvReader, "test", time.Now(), &l)
		require.NoError(t, err)
		if len(buff) == 0 {
			break
		}
		logger.Log.Infof("readed: %v", len(buff))
	}
}
