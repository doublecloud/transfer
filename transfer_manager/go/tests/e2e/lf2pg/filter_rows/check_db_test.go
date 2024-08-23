package jsonlogs

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers"
	jsonparser "github.com/doublecloud/transfer/transfer_manager/go/pkg/parsers/registry/json"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/logbroker"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/postgres"
	filterrows "github.com/doublecloud/transfer/transfer_manager/go/pkg/transformer/registry/filter_rows"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers"
	"github.com/doublecloud/transfer/transfer_manager/go/tests/helpers/lbenv"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/library/go/core/log"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

var (
	source = &logbroker.LfSource{}

	target = &postgres.PgDestination{
		Hosts:     []string{"localhost"},
		ClusterID: os.Getenv("TARGET_CLUSTER_ID"),
		User:      os.Getenv("PG_LOCAL_USER"),
		Password:  server.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:  os.Getenv("PG_LOCAL_DATABASE"),
		Port:      helpers.GetIntFromEnv("PG_LOCAL_PORT"),
	}
)

func TestReplication(t *testing.T) {
	sourceWriter, stop := initLfSource(t)
	defer stop()

	parserConfigStruct := &jsonparser.ParserConfigJSONLb{
		Fields: []abstract.ColSchema{
			{ColumnName: "msg", DataType: ytschema.TypeBytes.String()},
			{ColumnName: "id", DataType: ytschema.TypeInt32.String(), PrimaryKey: true},
			{ColumnName: "i64", DataType: ytschema.TypeInt64.String()},
			{ColumnName: "f64", DataType: ytschema.TypeFloat64.String()},
			{ColumnName: "str", DataType: ytschema.TypeString.String()},
			{ColumnName: "b", DataType: ytschema.TypeBoolean.String()},
			{ColumnName: "ts", DataType: ytschema.TypeTimestamp.String()},
			{ColumnName: "nil", DataType: ytschema.TypeString.String()},
			{ColumnName: "notnil", DataType: ytschema.TypeString.String()},
		},
		AddRest: false,
	}
	parserConfigMap, err := parsers.ParserConfigStructToMap(parserConfigStruct)
	require.NoError(t, err)
	source.ParserConfig = parserConfigMap

	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, abstract.TransferTypeIncrementOnly)
	go func() {
		sourceWriter.Info(
			"message",
			log.Int32("id", 1),
			log.Int64("i64", 9223372036854775807),
			log.Float64("f64", 0.1),
			log.String("str", "badname"),
			log.Bool("b", false),
			log.Time("ts", time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)),
		)
		sourceWriter.Info(
			"message",
			log.Int32("id", 2),
			log.Int64("i64", 200),
			log.Float64("f64", 0.2),
			log.String("str", "name"),
			log.Bool("b", true),
			log.Time("ts", time.Date(2010, time.January, 1, 0, 0, 0, 0, time.Local)),
			log.String("notnil", "str"),
		)
		sourceWriter.Info(
			"message",
			log.Int32("id", 3),
			log.Int64("i64", -9223372036854775808),
			log.Float64("f64", 0.3),
			log.String("str", "other"),
			log.Bool("b", true),
			log.Time("ts", time.Date(2005, time.January, 1, 0, 0, 0, 0, time.Local)),
			log.String("nil", "str"),
			log.String("notnil", "str"),
		)
		sourceWriter.Info(
			"message",
			log.Int32("id", 1),
			log.Int64("i64", 1),
			log.Float64("f64", 1),
			log.String("str", "1"),
			log.Bool("b", true),
			log.Time("ts", time.Date(2005, time.January, 1, 0, 0, 0, 0, time.Local)),
			log.String("nil", "str"),
			log.String("notnil", "str"),
		)
	}()

	// activate transfer
	filters := []string{
		strings.Join([]string{
			"id > 1",
			"i64 < 9223372036854775807", "i64 > -9223372036854775808",
			"f64 = 0.2",
			`str ~ "name"`, `str !~ "bad"`,
			"b = true",
			"ts = 2010-01-01T00:00:00",
			"nil = NULL",
			"notnil != NULL",
		}, " AND "),
		strings.Join([]string{
			"id = 1",
			"i64 = 1",
			"f64 = 1",
			`str = "1"`,
			"b = true",
			"nil != NULL",
			"notnil != NULL",
		}, " AND "),
	}

	transformer, err := filterrows.NewFilterRowsTransformer(
		filterrows.Config{Filters: filters},
		logger.Log,
	)
	require.NoError(t, err)
	require.NoError(t, transfer.AddExtraTransformer(transformer))

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	expected := []abstract.ChangeItem{{
		ColumnNames: []string{"id", "i64", "f64", "str", "b", "ts", "nil", "notnil"},
		ColumnValues: []interface{}{
			int32(2),
			int64(200),
			json.Number("0.2"),
			"name",
			true,
			time.Date(2010, time.January, 1, 0, 0, 0, 0, time.UTC),
			nil,
			"str",
		},
	}, {
		ColumnNames: []string{"id", "i64", "f64", "str", "b", "ts", "nil", "notnil"},
		ColumnValues: []interface{}{
			int32(1),
			int64(1),
			json.Number("1"),
			"1",
			true,
			time.Date(2005, time.January, 1, 0, 0, 0, 0, time.UTC),
			"str",
			"str",
		},
	}}

	dst := helpers.GetSampleableStorageByModel(t, target)
	err = helpers.WaitDestinationEqualRowsCount("public", source.Topics[0], dst, 300*time.Second, uint64(len(expected)))
	require.NoError(t, err)

	var actual []abstract.ChangeItem

	dst = helpers.GetSampleableStorageByModel(t, target)
	require.NoError(t, dst.LoadTable(context.Background(), abstract.TableDescription{
		Schema: "public",
		Name:   source.Topics[0],
	}, func(input []abstract.ChangeItem) error {
		for _, row := range input {
			if row.Kind != abstract.InsertKind {
				continue
			}
			item := abstract.ChangeItem{
				ColumnNames:  row.ColumnNames,
				ColumnValues: row.ColumnValues,
			}
			actual = append(actual, helpers.RemoveColumnsFromChangeItem(
				item, []string{"msg", "_partition", "_offset", "_idx", "_timestamp", "_rest"}))
		}
		return nil
	}))

	require.Equal(t, expected, actual)
}

func initLfSource(t *testing.T) (log.Logger, func()) {
	lbEnv, stop := lbenv.NewLbEnv(t)
	lbPort := lbEnv.ProducerOptions().Port
	sourcePort := lbEnv.Port

	sourceWriter, err := logger.NewLogbrokerLoggerFromConfig(&logger.LogbrokerConfig{
		Instance:    lbEnv.ProducerOptions().Endpoint,
		Port:        lbPort,
		Topic:       lbEnv.DefaultTopic,
		SourceID:    "test",
		Credentials: lbEnv.ProducerOptions().Credentials,
	}, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	source = &logbroker.LfSource{
		Instance:    logbroker.LogbrokerInstance(lbEnv.Endpoint),
		Topics:      []string{lbEnv.DefaultTopic},
		Credentials: lbEnv.ConsumerOptions().Credentials,
		Consumer:    lbEnv.DefaultConsumer,
		Port:        sourcePort,
	}
	return sourceWriter, stop
}
