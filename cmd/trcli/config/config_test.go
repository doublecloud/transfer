package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTransferYaml_WithEnvSubstitution(t *testing.T) {
	t.Setenv("FOO", "secret1")
	t.Setenv("BAR", "secret2")

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

	assert.Equal(t, "{\"Password\":\"secret1\"}", transfer.Src.Params)
	assert.Equal(t, "{\"Password\":\"secret2\"}", transfer.Dst.Params)
}

func TestParserTransferYaml_WithRawYaml(t *testing.T) {
	t.Setenv("FOO", "secret1")
	t.Setenv("BAR", "secret2")

	transfer, err := ParseTransferYaml([]byte(`
src:
  type: src_type
  params: |
    Password: ${FOO}
dst:
  type: dst_type
  params: |
    Password: ${BAR}
`))
	require.NoError(t, err)

	assert.Equal(t, "{\"Password\":\"secret1\"}", transfer.Src.Params)
	assert.Equal(t, "{\"Password\":\"secret2\"}", transfer.Dst.Params)
}

func TestParserTransferYaml_WithYaml(t *testing.T) {
	t.Setenv("FOO", "secret1")
	t.Setenv("BAR", "secret2")

	transfer, err := ParseTransferYaml([]byte(`
src:
  type: src_type
  params:
    Password: ${FOO}
dst:
  type: dst_type
  params:
    Password: ${BAR}
`))
	require.NoError(t, err)

	assert.Equal(t, "{\"Password\":\"secret1\"}", transfer.Src.Params)
	assert.Equal(t, "{\"Password\":\"secret2\"}", transfer.Dst.Params)
}
