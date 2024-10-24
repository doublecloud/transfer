package config

import (
	"github.com/stretchr/testify/require"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseTransferYaml_WithEnvSubstitution(t *testing.T) {

	require.NoError(t, os.Setenv("FOO", "secret1"))
	require.NoError(t, os.Setenv("BAR", "secret2"))
	defer os.Unsetenv("FOO")
	defer os.Unsetenv("BAR")

	transfer, err := ParseTransferYaml([]byte(`
src:
  type: src_type
  params: |
    {"Password": "${FOO}"}
dst:
  type: dst_type
  params: |
    {"Password": "${BAR}"}
`))
	require.NoError(t, err)

	assert.Equal(t, "{\"Password\": \"secret1\"}\n", transfer.Src.Params)
	assert.Equal(t, "{\"Password\": \"secret2\"}\n", transfer.Dst.Params)
}
