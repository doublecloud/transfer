package config

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"strings"
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
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}

	privateKeyPEM := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	}

	t.Setenv("FOO", "secret1")
	t.Setenv("BAR", "secret2")
	t.Setenv("PRIVATE_KEY", string(pem.EncodeToMemory(privateKeyPEM)))

	transfer, err := ParseTransferYaml([]byte(`
src:
  type: src_type
  params: |
    Password: ${FOO}
dst:
  type: dst_type
  params: |
    Password: ${BAR}
    tlsfile: ${PRIVATE_KEY}
`))
	require.NoError(t, err)

	assert.Equal(t, "{\"Password\":\"secret1\"}", transfer.Src.Params)

	expectTLSParams := strings.ReplaceAll(string(pem.EncodeToMemory(privateKeyPEM)), "\n", "\\n")
	assert.Equal(t, fmt.Sprintf("{\"Password\":\"secret2\",\"tlsfile\":\"%s\"}", expectTLSParams), transfer.Dst.Params)
}

func TestParserTransferYaml_WithYaml(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}

	privateKeyPEM := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	}

	t.Setenv("FOO", "secret1")
	t.Setenv("BAR", "secret2")
	t.Setenv("PRIVATE_KEY", string(pem.EncodeToMemory(privateKeyPEM)))

	transfer, err := ParseTransferYaml([]byte(`
src:
  type: src_type
  params:
    Password: ${FOO}
dst:
  type: dst_type
  params:
    Password: ${BAR}
    tlsfile: ${PRIVATE_KEY}
`))
	require.NoError(t, err)

	assert.Equal(t, "{\"Password\":\"secret1\"}", transfer.Src.Params)

	expectTLSParams := strings.ReplaceAll(string(pem.EncodeToMemory(privateKeyPEM)), "\n", "\\n")
	assert.Equal(t, fmt.Sprintf("{\"Password\":\"secret2\",\"tlsfile\":\"%s\"}", expectTLSParams), transfer.Dst.Params)
}
