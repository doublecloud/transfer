package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBreakURLIntoSubjectAndVersion(t *testing.T) {
	url, schemaID, err := extractSchemaIDAndURL("http://localhost:8081/schemas/ids/2")
	require.NoError(t, err)
	require.Equal(t, "http://localhost:8081", url)
	require.Equal(t, uint32(0x2), schemaID)
}
