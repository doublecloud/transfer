package confluent

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSchemasContainer(t *testing.T) {
	t.Run("empty container", func(t *testing.T) {
		schema := newSchemasContainer(nil)

		referenceName, subject, version := schema.getTask()
		require.Equal(t, referenceName, "")
		require.Equal(t, subject, "")
		require.Equal(t, version, int32(0))

		require.Equal(t, 0, len(schema.references()))
	})

	t.Run("simplest case, name == subject", func(t *testing.T) {
		schema := newSchemasContainer([]SchemaReference{
			{
				Name:        "0.proto",
				SubjectName: "0.proto",
				Version:     1,
			},
		})
		referenceName, subject, version := schema.getTask()
		require.Equal(t, referenceName, "0.proto")
		require.Equal(t, subject, "0.proto")
		require.Equal(t, version, int32(1))

		require.Error(t, schema.doneTask("unknown.proto", "unknown.proto", int32(1), Schema{}))
		require.NoError(t, schema.doneTask("0.proto", "0.proto", int32(1), Schema{}))

		referenceName, subject, version = schema.getTask()
		require.Equal(t, referenceName, "")
		require.Equal(t, subject, "")
		require.Equal(t, version, int32(0))

		require.Equal(t, 1, len(schema.references()))
	})

	t.Run("name != subject", func(t *testing.T) {
		schema := newSchemasContainer([]SchemaReference{
			{
				Name:        "market/x_func_tech/x_func_tech_jvm/temporal-business-logs-spring-boot-starter/proto/event_info.proto",
				SubjectName: "ru.yandex.market.x_func_tech.temporal.business.event_info",
				Version:     1,
			},
		})
		referenceName, subject, version := schema.getTask()
		require.Equal(t, referenceName, "market/x_func_tech/x_func_tech_jvm/temporal-business-logs-spring-boot-starter/proto/event_info.proto")
		require.Equal(t, subject, "ru.yandex.market.x_func_tech.temporal.business.event_info")
		require.Equal(t, version, int32(1))

		require.Error(t, schema.doneTask(
			"market/x_func_tech/x_func_tech_jvm/temporal-business-logs-spring-boot-starter/proto/event_info.proto",
			"unknown.proto",
			int32(1),
			Schema{},
		))
		require.NoError(t, schema.doneTask(
			"market/x_func_tech/x_func_tech_jvm/temporal-business-logs-spring-boot-starter/proto/event_info.proto",
			"ru.yandex.market.x_func_tech.temporal.business.event_info",
			int32(1),
			Schema{
				ID:         7,
				Schema:     "my_schema",
				SchemaType: PROTOBUF,
				References: nil,
			},
		))

		referenceName, subject, version = schema.getTask()
		require.Equal(t, referenceName, "")
		require.Equal(t, subject, "")
		require.Equal(t, version, int32(0))

		schemaReferences := schema.references()
		require.Equal(t, 1, len(schemaReferences))
		require.Equal(t, map[string]Schema{"market/x_func_tech/x_func_tech_jvm/temporal-business-logs-spring-boot-starter/proto/event_info.proto": {
			ID:         7,
			Schema:     "my_schema",
			SchemaType: PROTOBUF,
			References: nil,
		}}, schemaReferences)
	})
}
