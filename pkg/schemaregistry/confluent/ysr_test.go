package confluent

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsYSRHost(t *testing.T) {
	require.False(t, isYSRHost("https://my-lovely-host.yandex-team.ru"))
	require.False(t, isYSRHost("https://my-lovely-host.yandex-team.ru:443"))
	require.False(t, isYSRHost("https://127.0.0.1"))
	require.False(t, isYSRHost("https://1.2.3.4:443")) // like SchemaRegistry in managed-kafka - https://docs.yandex-team.ru/cloud/managed-kafka/concepts/managed-schema-registry#msr

	require.True(t, isYSRHost("https://srncq37em9mhuos49f33.schema-registry.yandex-team.ru"))
	require.True(t, isYSRHost("https://srncq37em9mhuos49f33.schema-registry-preprod.yandex-team.ru"))
}
