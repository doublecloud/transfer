package kafka

import (
	"os"
	"testing"

	"github.com/doublecloud/transfer/library/go/core/metrics/solomon"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/logbroker"
	"github.com/stretchr/testify/require"
)

// manual test based on YC junk cluster
// create kafka in YC - https://yc.yandex-team.ru/folders/mdb-junk/managed-kafka/cluster-create
// make sure u have topic foo_bar created and assigned to user
// set for example:
//	KAFKA_BROKER=man-98qecofs2eph1t8g.db.yandex.net:9091
//	KAFKA_USER=db_user
//	KAFKA_PASSWORD=P@ssword

func TestKafka(t *testing.T) {
	if os.Getenv("KAFKA_BROKER") == "" {
		t.SkipNow()
	}
	sinker, _ := NewReplicationSink(&KafkaDestination{
		Connection: &KafkaConnectionOptions{
			TLS:     logbroker.DefaultTLS,
			Brokers: []string{os.Getenv("KAFKA_BROKER")},
		},
		Auth: &KafkaAuth{
			Enabled:   true,
			Mechanism: "SHA-512",
			User:      os.Getenv("KAFKA_USER"),
			Password:  os.Getenv("KAFKA_PASSWORD"),
		},
		Topic: "foo_bar",
		FormatSettings: server.SerializationFormat{
			Name: server.SerializationFormatJSON,
		},
	}, solomon.NewRegistry(solomon.NewRegistryOpts()), logger.Log)

	err := sinker.Push([]abstract.ChangeItem{
		{
			LSN:          5,
			Kind:         abstract.InsertKind,
			Schema:       "foo",
			Table:        "bar",
			ColumnNames:  []string{"id", "val"},
			ColumnValues: []interface{}{int32(1), "old"},
			TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
				{ColumnName: "id", DataType: "int32"},
				{ColumnName: "val", DataType: "string"},
			}),
		},
		{
			LSN:          5,
			Kind:         abstract.InsertKind,
			Schema:       "foo",
			Table:        "bar",
			ColumnNames:  []string{"id", "val"},
			ColumnValues: []interface{}{int32(2), "old"},
			TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
				{ColumnName: "id", DataType: "int32"},
				{ColumnName: "val", DataType: "string"},
			}),
		},
	})
	require.NoError(t, err)
}
