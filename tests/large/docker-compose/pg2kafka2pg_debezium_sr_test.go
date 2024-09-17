package dockercompose

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/doublecloud/transfer/library/go/test/canon"
	"github.com/doublecloud/transfer/pkg/abstract"
	server "github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/debezium/parameters"
	"github.com/doublecloud/transfer/pkg/parsers"
	"github.com/doublecloud/transfer/pkg/parsers/registry/debezium"
	kafkasink "github.com/doublecloud/transfer/pkg/providers/kafka"
	"github.com/doublecloud/transfer/pkg/providers/postgres"
	"github.com/doublecloud/transfer/pkg/providers/postgres/pgrecipe"
	"github.com/doublecloud/transfer/tests/helpers"
	"github.com/stretchr/testify/require"
)

func TestPg2Kafka2PgSchemaRegistry(t *testing.T) {
	t.Parallel()

	const postgresPort = 6770
	const kafkaPort = 9092
	const schemaRegistryPort1 = 8081
	const schemaRegistryPort2 = 8083

	var kafkaBrokerAddress = fmt.Sprintf("localhost:%d", kafkaPort)
	var schemaRegistryURL = fmt.Sprintf("http://localhost:%d,http://localhost:%d", schemaRegistryPort1, schemaRegistryPort2)
	pgConnString := fmt.Sprintf("user=postgres dbname=postgres password=123 host=localhost port=%d", postgresPort)

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Postgres", Port: postgresPort},
			helpers.LabeledPort{Label: "Kafka", Port: kafkaPort},
			helpers.LabeledPort{Label: "Schema Registry", Port: schemaRegistryPort1},
		))
	}()

	var testCases = []struct {
		testName                   string
		serializerParams           map[string]string
		parserConfigSchemaRegistry string
	}{
		{"srTopicRecordNameStrategy",
			map[string]string{
				parameters.ValueConverter:                  parameters.ConverterConfluentJSON,
				parameters.ValueConverterSchemaRegistryURL: schemaRegistryURL,
				parameters.ValueSubjectNameStrategy:        parameters.SubjectTopicRecordNameStrategy,
				parameters.AddOriginalTypes:                parameters.BoolFalse,
			},
			schemaRegistryURL,
		},
		{"srRecordNameStrategy",
			map[string]string{
				parameters.ValueConverter:                  parameters.ConverterConfluentJSON,
				parameters.ValueConverterSchemaRegistryURL: schemaRegistryURL,
				parameters.ValueSubjectNameStrategy:        parameters.SubjectRecordNameStrategy,
				parameters.AddOriginalTypes:                parameters.BoolFalse,
			},
			schemaRegistryURL,
		},
		{"srTopicRecordNameStrategyOriginalTypes",
			map[string]string{
				parameters.ValueConverter:                  parameters.ConverterConfluentJSON,
				parameters.ValueConverterSchemaRegistryURL: schemaRegistryURL,
				parameters.ValueSubjectNameStrategy:        parameters.SubjectTopicRecordNameStrategy,
				parameters.AddOriginalTypes:                parameters.BoolTrue,
			},
			schemaRegistryURL,
		},
		{"srRecordNameStrategyOriginalTypes",
			map[string]string{
				parameters.ValueConverter:                  parameters.ConverterConfluentJSON,
				parameters.ValueConverterSchemaRegistryURL: schemaRegistryURL,
				parameters.ValueSubjectNameStrategy:        parameters.SubjectRecordNameStrategy,
				parameters.AddOriginalTypes:                parameters.BoolTrue,
			},
			schemaRegistryURL,
		},
		{"withoutSchemaRegistry",
			map[string]string{
				parameters.ValueConverter:              parameters.ConverterApacheKafkaJSON,
				parameters.AddOriginalTypes:            parameters.BoolTrue,
				parameters.ValueConverterSchemasEnable: parameters.BoolTrue,
			},
			"",
		},
	}
	for i := range testCases {
		func(i int) {
			t.Run(testCases[i].testName, func(t *testing.T) {
				t.Parallel()
				dbName := strings.ToLower(testCases[i].testName)
				// init databases
				pgrecipe.PgCreateDatabase(t,
					[]string{"docker", "run", "--network", "host", "registry.yandex.net/data-transfer/tests/base:1@sha256:48a92174b2d5917fbac6be0a48d974e3f836338acf4fa03f74fcfea7437386f1", "psql"},
					pgConnString, dbName, "postgres")
				// pg->kafka
				pgSource := postgres.PgSource{
					Hosts:    []string{"localhost"},
					User:     "postgres",
					Password: "123",
					Database: "postgres",
					DBTables: []string{"public.basic_types"},
					Port:     postgresPort,

					PgDumpCommand: dockerPgDump,
				}
				kafkaTarget := kafkasink.KafkaDestination{
					Connection: &kafkasink.KafkaConnectionOptions{
						TLS:     server.DisabledTLS,
						Brokers: []string{kafkaBrokerAddress},
					},
					Auth:  &kafkasink.KafkaAuth{Enabled: false},
					Topic: dbName,
					FormatSettings: server.SerializationFormat{
						Name:     server.SerializationFormatDebezium,
						Settings: testCases[i].serializerParams,
						BatchingSettings: &server.Batching{
							Enabled:        false,
							Interval:       0,
							MaxChangeItems: 0,
							MaxMessageSize: 0,
						},
					},
					ParralelWriterCount: 1,
				}
				//kafka->pg
				parserConfigMap, err := parsers.ParserConfigStructToMap(&debezium.ParserConfigDebeziumCommon{
					SchemaRegistryURL: testCases[i].parserConfigSchemaRegistry,
				})
				require.NoError(t, err)
				kafkaSource := kafkasink.KafkaSource{
					Connection: &kafkasink.KafkaConnectionOptions{
						TLS:     server.DisabledTLS,
						Brokers: []string{kafkaBrokerAddress},
					},
					Auth:             &kafkasink.KafkaAuth{Enabled: false},
					Topic:            dbName,
					Transformer:      nil,
					BufferSize:       server.BytesSize(1024),
					SecurityGroupIDs: nil,
					ParserConfig:     parserConfigMap,
					IsHomo:           false,
				}
				pgTarget := postgres.PgDestination{
					Database: dbName,
					User:     "postgres",
					Password: "123",
					Port:     postgresPort,
					Hosts:    []string{"localhost"},
					Cleanup:  server.Drop,
				}
				pg2kafka := helpers.MakeTransfer(dbName+"_pg_kafka", &pgSource, &kafkaTarget, abstract.TransferTypeSnapshotOnly)
				kafka2pg := helpers.MakeTransfer(dbName+"_kafka_pg", &kafkaSource, &pgTarget, abstract.TransferTypeIncrementOnly)
				w1 := helpers.Activate(t, pg2kafka)
				w2 := helpers.Activate(t, kafka2pg)
				require.NoError(t, helpers.WaitDestinationEqualRowsCount("public", "basic_types", helpers.GetSampleableStorageByModel(t, pgTarget), 60*time.Second, 1))
				w1.Close(t)
				w2.Close(t)

				if testCases[i].serializerParams[parameters.AddOriginalTypes] == parameters.BoolTrue {
					require.NoError(t, helpers.CompareStorages(t, pgSource, pgTarget, helpers.NewCompareStorageParams()))
				} else {
					canon.SaveJSON(t, pgrecipe.PgDump(
						t,
						[]string{"docker", "run", "--network", "host", "registry.yandex.net/data-transfer/tests/base:1@sha256:48a92174b2d5917fbac6be0a48d974e3f836338acf4fa03f74fcfea7437386f1", "pg_dump", "--table", "public.basic_types"},
						[]string{"docker", "run", "--network", "host", "registry.yandex.net/data-transfer/tests/base:1@sha256:48a92174b2d5917fbac6be0a48d974e3f836338acf4fa03f74fcfea7437386f1", "psql"},
						fmt.Sprintf("user=postgres dbname=%s password=123 host=localhost port=%d", dbName, postgresPort),
						"public.basic_types",
					))
				}

			})
		}(i)
	}
}
