package protoutil

import (
	"testing"

	"github.com/doublecloud/transfer/transfer_manager/go/pkg/util/gotest/test_data"
	"github.com/stretchr/testify/require"
)

func TestCopyWithoutSensitiveFieldsWithListsAndMaps(t *testing.T) {
	msg := test_data.WithListAndMap{
		Bar: "kek",
		ListSecrets: []*test_data.WithSecret{
			{
				Foo:      "list 1",
				Password: "list password 1",
			},
			{
				Foo:      "list 2",
				Password: "list password 2",
			},
		},
		MapSecrets: map[string]*test_data.WithSecret{
			"key 1": {
				Foo:      "map 1",
				Password: "map password 1",
			},
			"key 2": {
				Foo:      "map 2",
				Password: "map password 2",
			},
		},
		SecretList: []string{
			"secret list password 1",
			"secret list password 1",
		},
		SecretMap: map[string]string{
			"key 1": "secret map password 1",
			"key 2": "secret map password 2",
		},
		Secret: &test_data.WithSecret{
			Foo:      "x",
			Password: "password",
		},
	}

	logSafeMsg := CopyWithoutSensitiveFields(&msg)
	downcasted, ok := logSafeMsg.(*test_data.WithListAndMap)
	require.True(t, ok)
	require.Equal(t, "kek", downcasted.Bar)

	require.Len(t, downcasted.ListSecrets, 2)
	require.Equal(t, "list 1", downcasted.ListSecrets[0].Foo)
	require.Equal(t, "list 2", downcasted.ListSecrets[1].Foo)
	require.Equal(t, "", downcasted.ListSecrets[0].Password)
	require.Equal(t, "", downcasted.ListSecrets[1].Password)

	require.Len(t, downcasted.MapSecrets, 2)
	require.Equal(t, downcasted.MapSecrets["key 1"].Foo, "map 1")
	require.Equal(t, downcasted.MapSecrets["key 2"].Foo, "map 2")
	require.Equal(t, downcasted.MapSecrets["key 1"].Password, "")
	require.Equal(t, downcasted.MapSecrets["key 2"].Password, "")

	require.Empty(t, downcasted.SecretList)
	require.Empty(t, downcasted.SecretMap)
	require.Empty(t, downcasted.EmptyList)
	require.Empty(t, downcasted.EmptyMap)
	require.Empty(t, downcasted.EmptySecretList)
	require.Empty(t, downcasted.EmptySecretMap)

	require.Equal(t, "x", downcasted.Secret.Foo)
	require.Empty(t, downcasted.Secret.Password)
}
