package model

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMarshalUnmarshal(t *testing.T) {
	mockDestination := &MockDestination{
		Cleanup: Truncate,
	}
	mockDestinationBytes, err := json.Marshal(mockDestination)
	require.NoError(t, err)
	var mockDestinationResult MockDestination
	err = json.Unmarshal(mockDestinationBytes, &mockDestinationResult)
	require.NoError(t, err)
	require.Equal(t, Truncate, mockDestinationResult.Cleanup)
}
