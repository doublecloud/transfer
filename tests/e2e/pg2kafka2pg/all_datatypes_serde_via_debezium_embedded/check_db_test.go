package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/pkg/abstract"
	"github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/parsers"
	"github.com/doublecloud/transfer/pkg/parsers/registry/debezium"
	"github.com/doublecloud/transfer/pkg/providers/kafka"
	pgcommon "github.com/doublecloud/transfer/pkg/providers/postgres"
	"github.com/doublecloud/transfer/pkg/providers/postgres/pgrecipe"
	"github.com/doublecloud/transfer/tests/helpers"
	"github.com/stretchr/testify/require"
)

var (
	Source = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("init_source"))
	Target = *pgrecipe.RecipeTarget(pgrecipe.WithInitDir("init_target"))
)

var insertStmt = `
INSERT INTO public.basic_types VALUES (
    true,
    b'1',
    b'10101111',
    b'10101110',

    -32768,
    1,
    -8388605,
    0,
    1,
    3372036854775807,
    2,

    1.45e-10,
    3.14e-100,

    '1',
    'varchar_example',

    'abcd',
    'varc',
    '2004-10-19 10:23:54+02',
    '2004-10-19 11:23:54+02',
    '00:51:02.746572-08',
    '00:51:02.746572-08',
    interval '1 day 01:00:00',
    decode('CAFEBABE', 'hex'),

    '{"k1": "v1"}',
    '{"k2": "v2"}',
    '<foo>bar</foo>',

    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
    point(23.4, -44.5),
    '192.168.100.128/25',
    '[3,7)'::int4range,
    '[3,7)'::int8range,
    numrange(1.9,1.91),
    '[2010-01-02 10:00, 2010-01-02 11:00)',
    '[2010-01-01 01:00:00 -05, 2010-01-01 02:00:00 -08)'::tstzrange,
    daterange('2000-01-10'::date, '2000-01-20'::date, '[]'),

    1.45e-10,
    2,
    'text_example',

    -- ----------------------------------------------------------------------------------------------------------------

    --     DATE_ DATE,
    'January 8, 1999',

    --     TIME_ TIME,
    --     TIME1 TIME(1), -- precision: This is a fractional digits number placed in the seconds’ field. This can be up to six digits. HH:MM:SS.pppppp
    --     TIME6 TIME(6),
    '04:05:06',
    '04:05:06.1',
    '04:05:06.123456',

    --     TIMETZ__ TIME WITH TIME ZONE,
    --     TIMETZ1 TIME(1) WITH TIME ZONE,
    --     TIMETZ6 TIME(6) WITH TIME ZONE,
    '2020-05-26 13:30:25-04',
    '2020-05-26 13:30:25.5-04',
    '2020-05-26 13:30:25.575401-04',

    --     TIMESTAMP1 TIMESTAMP(1),
    --     TIMESTAMP6 TIMESTAMP(6),
    --     TIMESTAMP TIMESTAMP,
    '2004-10-19 10:23:54.9',
    '2004-10-19 10:23:54.987654',
    '2004-10-19 10:23:54',

    --
    --     NUMERIC_ NUMERIC,
    --     NUMERIC_5 NUMERIC(5),
    --     NUMERIC_5_2 NUMERIC(5,2),
    1267650600228229401496703205376,
    12345,
    123.67,

    --     DECIMAL_ DECIMAL,
    --     DECIMAL_5 DECIMAL(5),
    --     DECIMAL_5_2 DECIMAL(5,2),
    123456,
    12345,
    123.67,

    --     MONEY_ MONEY,
    99.98,

    --     HSTORE_ HSTORE,
    'a=>1,b=>2',

    --     INET_ INET,
    '192.168.1.5',

    --     CIDR_ CIDR,
    '10.1/16',

    --     MACADDR_ MACADDR,
    '08:00:2b:01:02:03',

    --     CITEXT_ CITEXT
    'Tom'
);
`

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

func TestSnapshotAndIncrement(t *testing.T) {
	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "PG source", Port: Source.Port},
	))
	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "PG source", Port: Source.Port},
		helpers.LabeledPort{Label: "PG target", Port: Target.Port},
	))

	//-----------------------------------------------------------------------------------------------------------------
	// pg -> kafka

	topic := "dbserver1"

	httpPort := os.Getenv("SR_HTTP_PORT")
	schemaRegistryURL := fmt.Sprintf("http://localhost:%s", httpPort)

	dst, err := kafka.DestinationRecipe()
	require.NoError(t, err)
	dst.Topic = topic
	dst.FormatSettings = model.SerializationFormat{
		Name: model.SerializationFormatDebezium,
		Settings: map[string]string{
			"value.converter":                               "io.confluent.connect.json.JsonSchemaConverter",
			"value.converter.schema.registry.url":           schemaRegistryURL,
			"value.converter.basic.auth.user.info":          "Oauth:blablabla",
			"value.converter.basic.auth.credentials.source": "USER_INFO",
			"dt.add.original.type.info":                     "true",
		},
	}
	dst.WithDefaults()

	helpers.InitSrcDst(helpers.TransferID, &Source, dst, abstract.TransferTypeSnapshotAndIncrement)
	transfer1 := &model.Transfer{
		ID:   "test_id_pg2lb",
		Src:  &Source,
		Dst:  dst,
		Type: abstract.TransferTypeSnapshotAndIncrement,
	}

	worker1 := helpers.Activate(t, transfer1)
	defer worker1.Close(t)

	//-----------------------------------------------------------------------------------------------------------------
	// kafka -> pg

	parserConfigStruct := &debezium.ParserConfigDebeziumCommon{
		SchemaRegistryURL: schemaRegistryURL,
		SkipAuth:          true,
		Username:          "",
		Password:          "",
		TLSFile:           "",
	}
	parserConfigMap, err := parsers.ParserConfigStructToMap(parserConfigStruct)
	require.NoError(t, err)

	src := &kafka.KafkaSource{
		Connection: &kafka.KafkaConnectionOptions{
			TLS:     model.DisabledTLS,
			Brokers: []string{os.Getenv("KAFKA_RECIPE_BROKER_LIST")},
		},
		Auth:             &kafka.KafkaAuth{Enabled: false},
		Topic:            topic,
		Transformer:      nil,
		BufferSize:       model.BytesSize(1024),
		SecurityGroupIDs: nil,
		ParserConfig:     parserConfigMap,
		IsHomo:           false,
	}
	src.WithDefaults()

	helpers.InitSrcDst(helpers.TransferID, src, &Target, abstract.TransferTypeIncrementOnly)
	transfer2 := &model.Transfer{
		ID:   "test_id_lb2yt",
		Src:  src,
		Dst:  &Target,
		Type: abstract.TransferTypeIncrementOnly,
	}

	worker2 := helpers.Activate(t, transfer2)
	defer worker2.Close(t)

	//-----------------------------------------------------------------------------------------------------------------

	time.Sleep(time.Second * 10)

	//---

	srcConn, err := pgcommon.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()

	_, err = srcConn.Exec(context.Background(), insertStmt)
	require.NoError(t, err)

	//---

	require.NoError(t, helpers.WaitDestinationEqualRowsCount("public", "basic_types", helpers.GetSampleableStorageByModel(t, Target), 60*time.Second, 2))
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}
